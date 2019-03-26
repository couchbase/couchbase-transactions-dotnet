using Couchbase.Core;

namespace Couchbase.Transactions
{
    public interface ITransactionDocument
    {
        string Key { get; }
        ulong Cas { get; set; }
        TransactionDocumentStatus Status { get; set; }
    }

    public interface ITransactionDocument<out T> : ITransactionDocument
    {
        T Content { get; }
    }

    internal interface ITransactionDocumentWithBucket
    {
        IBucket Bucket { get; }
        string BucketName { get; }
    }
}
