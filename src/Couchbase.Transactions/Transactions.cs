using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Deferred;

namespace Couchbase.Transactions
{
    public class Transactions : IDisposable
    {
        private static long InstancesCreated = 0;
        private static long InstancesCreatedDoingBackgroundCleanup = 0;
        private readonly ICluster _cluster;
        private bool _disposedValue;

        public TransactionConfig Config { get; }

        private Transactions(ICluster cluster, TransactionConfig config)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            Config = config ?? throw new ArgumentNullException(nameof(config));
            Interlocked.Increment(ref InstancesCreated);
            if (config.CleanupLostAttempts)
            {
                Interlocked.Increment(ref InstancesCreatedDoingBackgroundCleanup);
            }

            // TODO: kick off background cleanup "thread", if necessary
            // TODO: whatever the equivalent of 'cluster.environment().eventBus().publish(new TransactionsStarted(config));' is.
        }

        public static Transactions Create(ICluster cluster) => Create(cluster, TransactionConfigBuilder.Create().Build());
        public static Transactions Create(ICluster cluster, TransactionConfig config) => new Transactions(cluster, config);

        public static Transactions Create(ICluster cluster, TransactionConfigBuilder configBuilder) =>
            Create(cluster, configBuilder.Build());

        public Task<TransactionResult> Run(Action<AttemptContext> transactionLogic) => throw new NotImplementedException();
        public Task<TransactionResult> Run(Action<AttemptContext> transactionLogic, PerTransactionConfig perConfig) => throw new NotImplementedException();

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
