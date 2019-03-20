using System;
using System.Threading.Tasks;
using Couchbase.Transactions.Config;

namespace Couchbase.Transactions
{
    public interface ITransaction
    {
        TransactionConfig Config { get; }
        TimeSpan Duration { get; }
        Task<ITransactionResult> Run(Action<IAttemptContext> configureContext);
    }
}