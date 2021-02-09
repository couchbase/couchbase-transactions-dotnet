using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Couchbase.Core.Exceptions;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Core.Logging;
using Couchbase.Core.Retry;
using Couchbase.Transactions.Cleanup;
using Couchbase.Transactions.Cleanup.LostTransactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.DataAccess;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Error.Internal;
using Couchbase.Transactions.Internal;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Log;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Couchbase.Transactions
{
    public class Transactions : IDisposable, IAsyncDisposable
    {
        public static readonly TimeSpan OpRetryDelay = TimeSpan.FromMilliseconds(3);
        private static long InstancesCreated = 0;
        private static long InstancesCreatedDoingBackgroundCleanup = 0;
        private readonly ICluster _cluster;
        private bool _disposedValue;
        private readonly IRedactor _redactor;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger<Transactions> _logger;
        private readonly CleanupWorkQueue? _cleanupWorkQueue;
        private readonly ConcurrentQueue<CleanupRequest> _cleanupRequests = new ConcurrentQueue<CleanupRequest>();
        private readonly Cleaner _cleaner;
        private readonly IAsyncDisposable? _lostTransactionsCleanup;

        public TransactionConfig Config { get; }
        public string TransactionId { get; } = Guid.NewGuid().ToString();

        internal ICluster Cluster => _cluster;

        internal ITestHooks TestHooks { get; set; } = DefaultTestHooks.Instance;
        internal IDocumentRepository? DocumentRepository { get; set; } = null;
        internal IAtrRepository? AtrRepository { get; set; } = null;

        internal ICleanupTestHooks CleanupTestHooks
        {
            get => _cleaner.TestHooks;
            set => _cleaner.TestHooks = value;
        }

        private Transactions(ICluster cluster, TransactionConfig config)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            Config = config ?? throw new ArgumentNullException(nameof(config));
            _redactor = _cluster.ClusterServices?.GetService(typeof(IRedactor)) as IRedactor ?? throw new ArgumentNullException(nameof(IRedactor), "Redactor implementation not registered.");
            Interlocked.Increment(ref InstancesCreated);
            if (config.CleanupLostAttempts)
            {
                Interlocked.Increment(ref InstancesCreatedDoingBackgroundCleanup);
            }

            loggerFactory = config.LoggerFactory
                ?? _cluster.ClusterServices?.GetService(typeof(ILoggerFactory)) as ILoggerFactory
                ?? NullLoggerFactory.Instance;
            _logger = loggerFactory.CreateLogger<Transactions>();

            if (config.CleanupClientAttempts)
            {
                _cleanupWorkQueue = new CleanupWorkQueue(_cluster, Config.KeyValueTimeout);
            }
            _cleaner = new Cleaner(cluster, Config.KeyValueTimeout);

            if (config.CleanupLostAttempts)
            {
                _lostTransactionsCleanup = new LostTransactionManager(_cluster, loggerFactory, config.CleanupWindow, config.KeyValueTimeout);
            }

            // TODO: whatever the equivalent of 'cluster.environment().eventBus().publish(new TransactionsStarted(config));' is.
        }

        public static Transactions Create(ICluster cluster) => Create(cluster, TransactionConfigBuilder.Create().Build());
        public static Transactions Create(ICluster cluster, TransactionConfig config) => new Transactions(cluster, config);

        public static Transactions Create(ICluster cluster, TransactionConfigBuilder configBuilder) =>
            Create(cluster, configBuilder.Build());


        public Task<TransactionResult> RunAsync(Func<AttemptContext, Task> transactionLogic) =>
            RunAsync(transactionLogic, PerTransactionConfigBuilder.Create().Build());

        public async Task<TransactionResult> RunAsync(Func<AttemptContext, Task> transactionLogic, PerTransactionConfig perConfig)
        {
            // https://hackmd.io/foGjnSSIQmqfks2lXwNp8w?view#The-Core-Loop

            var overallContext = new TransactionContext(
                transactionId: Guid.NewGuid().ToString(),
                startTime: DateTimeOffset.UtcNow,
                config: Config,
                perConfig: perConfig
                );

            var result = new TransactionResult() { TransactionId =  overallContext.TransactionId };
            var attempts = new List<TransactionAttempt>();
            result.Attempts = attempts;

            var opRetryBackoffMillisecond = 1;
            var randomJitter = new Random();

            while (true)
            {
                try
                {
                    await ExecuteApplicationLambda(transactionLogic, overallContext, loggerFactory, attempts).CAF();
                    return result;
                }
                catch (TransactionOperationFailedException ex)
                {
                    // If anything above fails with error err
                    if (ex.RetryTransaction && !overallContext.IsExpired)
                    {
                        // If err.retry is true, and the transaction has not expired
                        //Apply OpRetryBackoff, with randomized jitter. E.g.each attempt will wait exponentially longer before retrying, up to a limit.
                        var jitter = randomJitter.Next(10);
                        var delayMs = opRetryBackoffMillisecond + jitter;
                        await Task.Delay(delayMs).CAF();
                        opRetryBackoffMillisecond = Math.Min(opRetryBackoffMillisecond * 10, 100);
                        //    Go back to the start of this loop, e.g.a new attempt.
                        continue;
                    }
                    else
                    {
                        // Otherwise, we are not going to retry. What happens next depends on err.raise
                        switch (ex.FinalErrorToRaise)
                        {
                            //  Failure post-commit may or may not be a failure to the application,
                            // as the cleanup process should complete the commit soon. It often depends on
                            // whether the application wants RYOW, e.g. AT_PLUS. So, success will be returned,
                            // but TransactionResult.unstagingComplete() will be false.
                            // The application can interpret this as it needs.
                            case TransactionOperationFailedException.FinalError.TransactionFailedPostCommit:
                                result.UnstagingComplete = false;
                                return result;

                            // Raise TransactionExpired to application, with a cause of err.cause.
                            case TransactionOperationFailedException.FinalError.TransactionExpired:
                                throw new TransactionExpiredException("Transaction Expired", ex.Cause, result);

                            // Raise TransactionCommitAmbiguous to application, with a cause of err.cause.
                            case TransactionOperationFailedException.FinalError.TransactionCommitAmbiguous:
                                throw new TransactionCommitAmbiguousException("Transaction may have failed to commit.", ex.Cause, result);

                            default:
                                throw new TransactionFailedException("Transaction failed.", ex.Cause, result);
                        }
                    }
                }
                catch (Exception notWrapped)
                {
                    // Assert err is an ErrorWrapper
                    throw new InvalidOperationException(
                        $"All exceptions should have been wrapped in an {nameof(TransactionOperationFailedException)}.",
                        notWrapped);
                }
            }

            throw new InvalidOperationException("Loop should not have exited without expiration.");
        }

        private async Task ExecuteApplicationLambda(Func<AttemptContext, Task> transactionLogic, TransactionContext overallContext, ILoggerFactory? loggerFactory,
            List<TransactionAttempt> attempts)
        {

            var attemptWatch = Stopwatch.StartNew();
            var ctx = new AttemptContext(
                overallContext,
                Config,
                Guid.NewGuid().ToString(),
                TestHooks,
                _redactor,
                loggerFactory,
                DocumentRepository,
                AtrRepository
            );

            try
            {
                try
                {
                    await transactionLogic(ctx).CAF();
                    await ctx.AutoCommit().CAF();
                    var attempt = ctx.ToAttempt();
                    attempt.TimeTaken = attemptWatch.Elapsed;
                    attempts.Add(attempt);
                }
                catch (TransactionOperationFailedException ex)
                {
                    // already a classified error
                    throw;
                }
                catch (Exception innerEx)
                {
                    // If err is not an ErrorWrapper, follow
                    // Exceptions Raised by the Application Lambda logic to turn it into one.
                    // From now on, all errors must be an ErrorWrapper.
                    // https://hackmd.io/foGjnSSIQmqfks2lXwNp8w?view#Exceptions-Raised-by-the-Application-Lambda
                    var error = ErrorBuilder.CreateError(ctx, innerEx.Classify()).Cause(innerEx);
                    if (innerEx is IRetryable)
                    {
                        error.RetryTransaction();
                    }

                    throw error.Build();
                }
            }
            catch (TransactionOperationFailedException ex)
            {
                // If err.rollback is true (it generally will be), auto-rollback the attempt by calling rollbackInternal with appRollback=false.
                try
                {
                    if (ex.AutoRollbackAttempt)
                    {
                        try
                        {
                            _logger.LogWarning("Attempt failed, attempting automatic rollback...");
                            await ctx.RollbackInternal(isAppRollback: false).CAF();
                        }
                        catch (Exception rollbackEx)
                        {
                            _logger.LogWarning("Rollback failed due to {reason}", rollbackEx.Message);
                            // if rollback failed, raise the original error, but with retry disabled:
                            // Error(ec = err.ec, cause = err.cause, raise = err.raise
                            throw ErrorBuilder.CreateError(ctx, ex.CausingErrorClass)
                                .Cause(ex.Cause)
                                .DoNotRollbackAttempt()
                                .RaiseException(ex.FinalErrorToRaise)
                                .Build();
                        }
                    }

                    // If the transaction has expired, raised Error(ec = FAIL_EXPIRY, rollback=false, raise = TRANSACTION_EXPIRED)
                    if (overallContext.IsExpired)
                    {
                        _logger.LogWarning("Transaction is expired.  No more retries or rollbacks.");
                        throw ErrorBuilder.CreateError(ctx, ErrorClass.FailExpiry, ex)
                            .DoNotRollbackAttempt()
                            .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired)
                            .Build();
                    }
                }
                finally
                {
                    // Whether this fails or succeeds
                    //  Add a TransactionAttempt that will be returned in the final TransactionResult.
                    //  AddCleanupRequest, if the cleanup thread is configured to be running.
                    var errAttempt = ctx.ToAttempt();
                    errAttempt.TerminatedByException = ex;
                    errAttempt.TimeTaken = attemptWatch.Elapsed;
                    attempts.Add(errAttempt);
                }

                // Else if it succeeded or no rollback was performed, propagate err up.
                _logger.LogDebug("Propagating error up. (ec = {ec}, retry = {retry}, finalError = {finalError})", ex.CausingErrorClass, ex.RetryTransaction, ex.FinalErrorToRaise);
                throw;
            }
            finally
            {
                AddCleanupRequest(ctx);
            }
        }

        private void AddCleanupRequest(AttemptContext ctx)
        {
            // TODO: Implement config option for background cleanup.
            var cleanupRequest = ctx.GetCleanupRequest();
            if (cleanupRequest != null)
            {
                _cleanupRequests.Enqueue(cleanupRequest);
                if (_cleanupWorkQueue?.TryAddCleanupRequest(cleanupRequest) == false)
                {
                    _logger.LogWarning("Failed to add background cleanup request: {req}", cleanupRequest);
                }
            }
        }

        private async Task CleanupAttempts()
        {
            foreach (var cleanupRequest in _cleanupRequests)
            {
                await _cleaner.ProcessCleanupRequest(cleanupRequest).CAF();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                _disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~Transactions()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            if (Config.CleanupClientAttempts)
            {
                await CleanupAttempts().CAF();
            }

            if (_lostTransactionsCleanup != null)
            {
                await _lostTransactionsCleanup.DisposeAsync().CAF();
            }

            Dispose();
        }
    }
}
