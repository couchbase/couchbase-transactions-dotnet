using System;
using System.Threading.Tasks;
using Couchbase.Transactions.old.Config;

namespace Couchbase.Transactions.old
{
    public interface ITransactionClient
    {
        TransactionConfig Config { get; }
        TimeSpan Duration { get; }
        Task<ITransactionResult> Run(Func<IAttemptContext, Task> transactionLogic);
    }
}
