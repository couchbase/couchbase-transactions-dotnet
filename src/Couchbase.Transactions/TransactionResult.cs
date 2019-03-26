using System;
using System.Collections.Generic;

namespace Couchbase.Transactions
{
    internal class TransactionResult : ITransactionResult
    {
        public string TransactionId { get; }
        public IReadOnlyCollection<ITransactionAttempt> Attempts { get; }
        public TimeSpan Duration { get; }

        public TransactionResult(string transactionId, IReadOnlyCollection<ITransactionAttempt> attempts, TimeSpan duration)
        {
            TransactionId = transactionId;
            Attempts = attempts;
            Duration = duration;
        }
    }
}
