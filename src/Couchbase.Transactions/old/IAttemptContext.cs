using System.Threading.Tasks;
using Couchbase.Core;

namespace Couchbase.Transactions.old
{
    public interface IAttemptContext
    {
        string TransactionId { get; }
        string AttemptId { get; }

        Task<ITransactionDocument<T>> Get<T>(IBucket bucket, string key);
        Task<ITransactionDocument<T>> Insert<T>(IBucket bucket, string key, T content);
        Task<ITransactionDocument<T>> Replace<T>(IBucket bucket, ITransactionDocument<T> document);
        Task Remove(IBucket bucket, ITransactionDocument document);

        Task Commit();
        Task Rollback();
    }
}
