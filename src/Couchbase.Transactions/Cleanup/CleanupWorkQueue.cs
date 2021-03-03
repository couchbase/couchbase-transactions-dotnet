using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Support;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Cleanup
{
    internal class CleanupWorkQueue : IDisposable
    {
        // CBD-3677
        public const int MaxCleanupQueueDepth = 10_000;
        private readonly CancellationTokenSource _forceFlush = new CancellationTokenSource();

        // TODO: This needs to be bounded, with a circuit breaker.
        private readonly BlockingCollection<CleanupRequest> _workQueue = new BlockingCollection<CleanupRequest>(MaxCleanupQueueDepth);
        private readonly Task _consumer;
        private readonly Cleaner _cleaner;

        private ICleanupTestHooks _testHooks = DefaultCleanupTestHooks.Instance;
        public ICleanupTestHooks TestHooks
        {
            get => _testHooks;
            set
            {
                _testHooks = value;
                _cleaner.TestHooks = value;
            }
        }

        public CleanupWorkQueue(ICluster cluster, TimeSpan? keyValueTimeout, ILoggerFactory loggerFactory, bool runCleanup)
        {
            _cleaner = new Cleaner(cluster, keyValueTimeout, loggerFactory, creatorName: nameof(CleanupWorkQueue)) { TestHooks = TestHooks };
            _consumer = runCleanup ? Task.Run(ConsumeWork) : Task.CompletedTask;
        }

        public IEnumerable<CleanupRequest> RemainingCleanupRequests => _workQueue.ToArray();

        internal bool TryAddCleanupRequest(CleanupRequest cleanupRequest) => _workQueue.TryAdd(cleanupRequest);

        private async Task ConsumeWork()
        {
            // Initial, naive implementation.
            // Single-threaded consumer that assumes cleanupRequests are in order of transaction expiry already
            foreach (var cleanupRequest in _workQueue.GetConsumingEnumerable())
            {
                var delay = cleanupRequest.WhenReadyToBeProcessed - DateTimeOffset.UtcNow;
                if (delay > TimeSpan.Zero && !_forceFlush.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(delay, _forceFlush.Token).CAF();
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }

                try
                {
                    var cleanupResult = await _cleaner.ProcessCleanupRequest(cleanupRequest).ConfigureAwait(false);
                    if (!cleanupResult.Success && cleanupResult.FailureReason != null)
                    {
                        throw new CleanupFailedException(cleanupResult.FailureReason);
                    }
                }
                catch (Exception ex)
                {
                    // TODO:  need a more intelligent error handling.
                    cleanupRequest.ProcessingErrors.Enqueue(ex);
                    if (cleanupRequest.ProcessingErrors.Count > 100)
                    {
                        // TODO: put it in a dead queue for special handling.
                        // drop the request, for now
                        return;
                    }

                    // retry in 10 seconds plus some jitter
                    var updatedCleanupRequest = cleanupRequest with { WhenReadyToBeProcessed = DateTimeOffset.UtcNow.AddSeconds(10).AddMilliseconds(DateTime.UtcNow.Second) };
                    TryAddCleanupRequest(updatedCleanupRequest);
                }
            }
        }

        /// <summary>
        /// Call during app shutdown to finish all cleanup request as soon as possible.
        /// </summary>
        /// <returns>A Task representing asynchronous work.</returns>
        internal async Task ForceFlushAsync()
        {
            StopProcessing();
            await _consumer;
        }

        private void StopProcessing()
        {
            try
            {
                _forceFlush.Cancel(throwOnFirstException: false);
                _workQueue.CompleteAdding();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void Dispose()
        {
            StopProcessing();
            _workQueue.Dispose();
        }
    }
}
