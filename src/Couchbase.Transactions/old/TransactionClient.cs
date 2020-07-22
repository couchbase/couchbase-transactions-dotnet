using System;
using System.Threading.Tasks;
using Couchbase.Core;
using Couchbase.Transactions.old.Config;
using Polly;

namespace Couchbase.Transactions.old
{
    public class TransactionClient : ITransactionClient
    {
        private readonly ICluster _cluster;
        private readonly DateTime _startedAt;
        private readonly Policy _policy;

        public TransactionConfig Config { get; }
        public TimeSpan Duration => DateTime.UtcNow.Subtract(_startedAt);

        public TransactionClient(ICluster cluster, TransactionConfig config)
        {
            _cluster = cluster;
            _startedAt = DateTime.UtcNow;
            Config = config;

            // setup retry policy using Polly
            //TODO: handle retryable exceptions differently
            _policy = Policy.Handle<Exception>().Retry(Config.MaxAttempts);
        }

        public async Task<ITransactionResult> Run(Func<IAttemptContext, Task> transactionLogic)
        {
            var transactionContext = new TransactionContext();

            await _policy.Execute(async () =>
            {
                var attemptContext = new AttemptContext(transactionContext.TransactionId, Config);
                await transactionLogic(attemptContext).ConfigureAwait(false);
            }).ConfigureAwait(false);

            return new TransactionResult(transactionContext.TransactionId, transactionContext.Attempts, Duration);
        }
    }
}
