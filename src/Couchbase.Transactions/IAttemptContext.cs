using System.Threading.Tasks;
using Couchbase.Core;

namespace Couchbase.Transactions
{
    public interface IAttemptContext
    {
        string AttemptId { get; }
        string TransactionId { get; }

        Task<ITransactionResult> Get(IBucket bucket, string key);
        Task<ITransactionResult> Insert<T>(IBucket bucket, string key, T document);
        Task<ITransactionResult> Replace<T>(IBucket bucket, string key, T document);
        Task<ITransactionResult> Remove(IBucket bucket, string key);

        Task Commit();
        Task Rollback();
    }
}