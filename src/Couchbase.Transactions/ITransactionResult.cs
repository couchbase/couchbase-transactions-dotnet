using System;
using System.Collections.Generic;

namespace Couchbase.Transactions
{
    public interface ITransactionResult
    {
        string TransactionId { get; }
        TimeSpan Duration { get; }
        IReadOnlyCollection<ITransactionAttempt> Attempts { get; }
    }
}
