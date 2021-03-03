using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataAccess;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.Internal;
using Couchbase.Transactions.Internal.Test;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions.Cleanup.LostTransactions
{
    internal class PerBucketCleaner : IAsyncDisposable
    {
        public string ClientUuid { get; }

        private readonly Cleaner _cleaner;
        private readonly ICleanerRepository _repository;
        private readonly TimeSpan _cleanupWindow;
        private readonly ILogger<PerBucketCleaner> _logger;
        private readonly Timer _processCleanupTimer;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Random _jitter = new Random();
        private readonly SemaphoreSlim _timerCallbackMutex = new SemaphoreSlim(1);
        private long _runCount = 0;
        public ICleanupTestHooks TestHooks { get; set; } = DefaultCleanupTestHooks.Instance;
        public long RunCount => Interlocked.Read(ref _runCount);
        public bool Running => !_cts.IsCancellationRequested;

        public PerBucketCleaner(string clientUuid, Cleaner cleaner, ICleanerRepository repository,TimeSpan cleanupWindow, ILoggerFactory loggerFactory, bool startDisabled = false)
        {
            ClientUuid = clientUuid;
            _cleaner = cleaner; // TODO: Cleaner should have its data access refactored into ICleanerRepository, and then that should be made a property, eliminating the need for a _repository variable here.
            _repository = repository;
            _cleanupWindow = cleanupWindow;
            _logger = loggerFactory.CreateLogger<PerBucketCleaner>();
            _processCleanupTimer = new System.Threading.Timer(
                callback: TimerCallback,
                state: null,
                dueTime: startDisabled ? -1 : 0,
                period: (int)cleanupWindow.TotalMilliseconds);

            FullBucketName = (bucket: BucketName, scope: ScopeName, collection: CollectionName, clientUuid: ClientUuid).ToString();
        }

        public void Start()
        {
            _processCleanupTimer.Change(TimeSpan.Zero, _cleanupWindow);
        }

        public void Stop()
        {
            _processCleanupTimer.Change(-1, (int)_cleanupWindow.TotalMilliseconds);
        }

        public string BucketName => _repository.BucketName;
        public string ScopeName => _repository.ScopeName;
        public string CollectionName => _repository.CollectionName;

        public string FullBucketName { get; }

        public override string ToString()
        {
            return new Summary(FullBucketName, ClientUuid, Running, RunCount).ToString();
        }

        private record Summary(string FullBucketName, string ClientUuid, bool Running, long RunCount);

        public void Dispose()
        {
            Stop();
            if (!_cts.IsCancellationRequested)
            {
                _processCleanupTimer.Change(-1, -1);
                _processCleanupTimer.Dispose();
                _cts.Cancel();
            }
        }


        public async ValueTask DisposeAsync()
        {
            _logger.LogDebug("Disposing {bkt}", FullBucketName);
            Dispose();
            await RemoveClient().CAF();
        }

        private async void TimerCallback(object? state)
        {
            if (_cts.IsCancellationRequested)
            {
                return;
            }

            var enteredWithoutTimeout = await _timerCallbackMutex.WaitAsync(_cleanupWindow).CAF();
            if (!enteredWithoutTimeout)
            {
                _logger.LogDebug("Timed out while waiting for overlapping callbacks on {bkt}", FullBucketName);
                return;
            }

            try
            {
                _ = await ProcessClient().CAF();
            }
            catch (Exception ex)
            {
                // TODO: If BF-CBD-3794 and the error is an AccessError, silently abort the thread.
                _logger.LogWarning("Processing of bucket '{bkt}' failed unexpectedly: {ex}", FullBucketName, ex);
            }
            finally
            {
                _timerCallbackMutex.Release();
            }
        }

        // method referred to as "Per Bucket Algo" in the RFC
        internal async Task<ClientRecordDetails> ProcessClient(bool cleanupAtrs = true)
        {
            _logger.LogDebug("Looking for lost transactions on bucket '{bkt}'", FullBucketName);
            ClientRecordDetails clientRecordDetails = await EnsureClientRecordIsUpToDate();
            if (clientRecordDetails.OverrideActive)
            {
                _logger.LogInformation("Cleanup of '{bkt}' is currently disabled by another actor.", FullBucketName);
                return clientRecordDetails;
            }

            using var boundedCleanup = new CancellationTokenSource(_cleanupWindow);
            using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(boundedCleanup.Token, _cts.Token);

            // for fit tests, we may want to manipulate the client records without actually bothering with cleanup.
            if (cleanupAtrs)
            {
                // we may not have enough time to process every ATR in the configured window.  Process a random member.
                var randomAtrs = clientRecordDetails.AtrsHandledByThisClient.ToList();
                var rnd = new Random();
                randomAtrs.Sort((a, b) => (rnd.Next() % 2 == 0 ? 1 : -1));
                foreach (var atrId in randomAtrs)
                {
                    if (linkedSource.IsCancellationRequested)
                    {
                        _logger.LogDebug("Exiting cleanup of ATR {atr} on {bkt} early due to cancellation.", atrId, FullBucketName);
                        break;
                    }

                    // Every checkAtrEveryNMillis, handle an ATR with id atrId
                    await CleanupAtr(atrId, linkedSource.Token).CAF();
                    Interlocked.Increment(ref _runCount);
                    await Task.Delay(clientRecordDetails.CheckAtrTimeWindow).CAF();
                }
            }

            return clientRecordDetails;
        }

        private async Task<ClientRecordDetails> EnsureClientRecordIsUpToDate()
        {
            ClientRecordDetails? clientRecordDetails = null;
            bool repeat;
            do
            {
                ulong? pathnotFoundCas = null;
                try
                {
                    // Parse the client record.
                    await TestHooks.BeforeGetRecord(ClientUuid).CAF();
                    (ClientRecordsIndex? clientRecord, ParsedHLC parsedHlc, ulong? cas) = await _repository.GetClientRecord().CAF();
                    if (clientRecord == null)
                    {
                        _logger.LogDebug("No client record found on '{bkt}', cas = {cas}", this, cas);
                        pathnotFoundCas = cas;
                        throw new LostCleanupFailedException("No existing Client Record.") { CausingErrorClass = ErrorClass.FailDocNotFound };
                    }

                    clientRecordDetails = new ClientRecordDetails(clientRecord, parsedHlc, ClientUuid, _cleanupWindow);
                    _logger.LogDebug("Found client record for '{bkt}':\n{clientRecordDetails}", FullBucketName, clientRecordDetails);
                    break;
                }
                catch (Exception ex)
                {
                    (var handled, var repeatAfterGetRecord) = await HandleGetRecordFailure(ex, pathnotFoundCas).CAF();
                    repeat = repeatAfterGetRecord;

                    if (!handled)
                    {
                        throw;
                    }
                    else
                    {
                        continue;
                    }
                }
            }
            while (repeat && !_cts.Token.IsCancellationRequested);

            if (clientRecordDetails == null)
            {
                throw new InvalidOperationException(nameof(clientRecordDetails) + " should have been assigned by this point.");
            }

            // NOTE: The RFC says to retry with an exponential backoff, but neither the java implementation nor the FIT tests agree with that.
            await TestHooks.BeforeUpdateRecord(ClientUuid);
            await _repository.UpdateClientRecord(ClientUuid, _cleanupWindow, ActiveTransactionRecords.AtrIds.NumAtrs, clientRecordDetails.ExpiredClientIds).CAF();
            _logger.LogDebug("Successfully updated Client Record Entry for {clientUuid} on {bkt}", ClientUuid, FullBucketName);

            return clientRecordDetails;
        }

        private async Task<(bool handled, bool repeatProcessClient)> HandleGetRecordFailure(Exception ex, ulong? pathNotFoundCas)
        {
            var ec = ex.Classify();
            switch (ec)
            {
                case ErrorClass.FailDocNotFound:
                    try
                    {
                        // Client record needs to be created.
                        await TestHooks.BeforeCreateRecord(ClientUuid).CAF();
                        await _repository.CreatePlaceholderClientRecord(pathNotFoundCas).CAF();
                        _logger.LogDebug("Created placeholder Client Record for '{bkt}', cas = {cas}", FullBucketName, pathNotFoundCas);

                        // On success, call the processClient algo again.
                        return (handled: true, repeatProcessClient: true);
                    }
                    catch (Exception exCreatePlaceholder)
                    {
                        var ecCreatePlaceholder = exCreatePlaceholder.Classify();
                        switch (ecCreatePlaceholder)
                        {
                            case ErrorClass.FailDocAlreadyExists:
                                // continue as success
                                return (handled: true, repeatProcessClient: false);
                            case ErrorClass.FailCasMismatch:
                                _logger.LogWarning("Should not have hit CasMismatch for case FailDocNotFound when creating placeholder client record for {bkt}", FullBucketName);
                                throw;
                            // TODO: Else if BF-CBD-3794, and err indicates a NO_ACCESS
                            default:
                                throw;
                        }
                    }
                // TODO: Handle NoAccess BF-CBD-3794,
                default:
                    // Any other error, propagate it.
                    return (handled: false, repeatProcessClient: false);
            }
        }

        private async Task CleanupAtr(string atrId, CancellationToken cancellationToken)
        {
            _logger.LogDebug("{this} Cleaning up ATR {atrId}", this, atrId);
            Dictionary<string, AtrEntry> attempts;
            ParsedHLC parsedHlc;
            try
            {
                await TestHooks.BeforeAtrGet(atrId).CAF();
                (attempts, parsedHlc) = await _repository.LookupAttempts(atrId).CAF();
            }
            catch (Exception ex)
            {
                var ec = ex.Classify();
                switch (ec)
                {
                    case ErrorClass.FailDocNotFound:
                    case ErrorClass.FailPathNotFound:
                        // If the ATR is not present, continue as success.
                        return;
                    default:
                        // Else if there’s an error, continue as success.
                        _logger.LogWarning("Failed to look up attempts on ATR {atrId}: {ex}", atrId, ex);
                        return;
                }
            }

            foreach (var kvp in attempts)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug("Exiting cleanup of attempt {attempt} on {bkt} early due to cancellation.", kvp.Key, FullBucketName);
                    return;
                }

                (var attemptId, var attempt) = (kvp.Key, kvp.Value);
                if (attempt == null)
                {
                    continue;
                }

                var isExpired = attempt?.TimestampStartMsecs.HasValue == true
                    && attempt?.ExpiresAfterMsecs.HasValue == true
                    && attempt.TimestampStartMsecs!.Value.AddMilliseconds(attempt.ExpiresAfterMsecs!.Value) < parsedHlc.NowTime;
                if (isExpired)
                {
                    var atrCollection = await AtrRepository.GetAtrCollection(new AtrRef()
                    {
                        BucketName = BucketName,
                        ScopeName = ScopeName,
                        CollectionName = CollectionName,
                        Id = atrId
                    }, _repository.Collection);

                    if (atrCollection == null)
                    {
                        continue;
                    }

                    var cleanupRequest = new CleanupRequest(
                        AttemptId: attemptId,
                        AtrId: atrId,
                        AtrCollection: atrCollection,
                        InsertedIds: attempt!.InsertedIds.ToList(),
                        RemovedIds: attempt.RemovedIds.ToList(),
                        ReplacedIds: attempt.ReplacedIds.ToList(),
                        State: attempt.State,
                        WhenReadyToBeProcessed: DateTimeOffset.UtcNow,
                        ProcessingErrors: new ConcurrentQueue<Exception>());

                    if (_cts.IsCancellationRequested)
                    {
                        return;
                    }

                    await _cleaner.ProcessCleanupRequest(cleanupRequest, isRegular: false);
                }
            }
        }


        private async Task RemoveClient()
        {
            var retryDelay = 1;
            for (int retryCount = 1; retryDelay <= 250; retryCount++)
            {
                retryDelay = (int)Math.Pow(2, retryCount) + _jitter.Next(10);
                try
                {
                    await TestHooks.BeforeRemoveClient(ClientUuid).CAF();
                    await _repository.RemoveClient(ClientUuid).CAF();
                    _logger.LogDebug("Removed client {clientUuid} for {bkt}", ClientUuid, FullBucketName);
                    return;
                }
                catch (ObjectDisposedException)
                {
                    _logger.LogDebug("Cannot continue cleanup after underlying data access has been disposed for {bkt}", FullBucketName);
                    return;
                }
                catch (Exception ex)
                {
                    var ec = ex.Classify();
                    switch (ec)
                    {
                        case ErrorClass.FailDocNotFound:
                        case ErrorClass.FailPathNotFound:
                            // treat as success
                            _logger.LogInformation("{ec} ignored during Remove Lost Transaction Client.", ec);
                            return;
                        default:
                            _logger.LogDebug("{ec} during Remove Lost Transaction Client, retryCount = {rc}", ec, retryCount);
                            await Task.Delay(retryDelay).CAF();
                            break;
                    }
                }
            }
        }
    }
}
