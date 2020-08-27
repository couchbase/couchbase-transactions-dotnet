using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Couchbase.Core.Exceptions;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Core.Logging;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Deferred;
using Couchbase.Transactions.Internal;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Log;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions
{
    public class Transactions : IDisposable
    {
        private static long InstancesCreated = 0;
        private static long InstancesCreatedDoingBackgroundCleanup = 0;
        private readonly ICluster _cluster;
        private bool _disposedValue;
        private readonly IRedactor _redactor;
        private readonly ILoggerFactory loggerFactory;

        public TransactionConfig Config { get; }

        private readonly ITypeTranscoder _typeTranscoder;

        public string TransactionId { get; } = Guid.NewGuid().ToString();

        internal ICluster Cluster => _cluster;

        public ITestHooks TestHooks { get; set; } = DefaultTestHooks.Instance;

        private Transactions(ICluster cluster, TransactionConfig config)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            Config = config ?? throw new ArgumentNullException(nameof(config));
            _typeTranscoder = _cluster.ClusterServices?.GetService(typeof(ITypeTranscoder)) as ITypeTranscoder ??
                              throw new InvalidArgumentException($"{nameof(ITypeTranscoder)}Necessary type not registered.");
            _redactor = _cluster.ClusterServices?.GetService(typeof(IRedactor)) as IRedactor ?? DefaultRedactor.Instance;
            Interlocked.Increment(ref InstancesCreated);
            if (config.CleanupLostAttempts)
            {
                Interlocked.Increment(ref InstancesCreatedDoingBackgroundCleanup);
            }

           loggerFactory = _cluster.ClusterServices.GetService(typeof(ILoggerFactory)) as ILoggerFactory;

            // TODO: kick off background cleanup "thread", if necessary
            // TODO: whatever the equivalent of 'cluster.environment().eventBus().publish(new TransactionsStarted(config));' is.
        }

        public static Transactions Create(ICluster cluster) => Create(cluster, TransactionConfigBuilder.Create().Build());
        public static Transactions Create(ICluster cluster, TransactionConfig config) => new Transactions(cluster, config);

        public static Transactions Create(ICluster cluster, TransactionConfigBuilder configBuilder) =>
            Create(cluster, configBuilder.Build());

        public Task<TransactionResult> Run(Func<AttemptContext, Task> transactionLogic) =>
            Run(transactionLogic, PerTransactionConfigBuilder.Create().Build());

        public async Task<TransactionResult> Run(Func<AttemptContext, Task> transactionLogic, PerTransactionConfig perConfig)
        {
            // TODO: placeholder before TXNN-5: Implement Core Loop
            // real loop will run multiple attempts with retries
            
            var overallContext = new TransactionContext(
                transactionId: Guid.NewGuid().ToString(),
                startTime: DateTimeOffset.UtcNow,
                config: Config,
                perConfig: perConfig
                );

            ILoggerFactory? loggerFactory = null;


            var result = new TransactionResult() { TransactionId =  overallContext.TransactionId };
            var attempts = new List<TransactionAttempt>();
            result.Attempts = attempts;

            // TODO: retry according to spec
            for (int i = 0; i < 3; i++)
            {
                var ctx = new AttemptContext(
                    overallContext,
                    Config,
                    Guid.NewGuid().ToString(),
                    this,
                    TestHooks,
                    _redactor,
                    _typeTranscoder,
                    loggerFactory
                );

                // TODO: capture exception.
                try
                {
                    await transactionLogic(ctx).CAF();
                    await ctx.AutoCommit().CAF();
                    var attempt = ctx.ToAttempt();
                    attempts.Add(attempt);
                    break;
                }
                catch (Exception ex)
                {
                    var errAttempt = ctx.ToAttempt();
                    errAttempt.TermindatedByException = ex;
                    attempts.Add(errAttempt);
                }
            }

            return result;
        }

        public Task<TransactionResult> Commit(TransactionSerializedContext serialized, PerTransactionConfig perConfig) => throw new NotImplementedException();
        public Task<TransactionResult> RollBack(TransactionSerializedContext serialized, PerTransactionConfig perConfig) => throw new NotImplementedException();

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
    }
}
