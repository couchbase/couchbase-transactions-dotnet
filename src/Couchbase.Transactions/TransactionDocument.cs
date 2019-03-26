using Couchbase.Core;

namespace Couchbase.Transactions
{
    internal class TransactionDocument<T> : ITransactionDocument<T>, ITransactionDocumentWithBucket
    {
        public IBucket Bucket { get; }
        public string BucketName => Bucket.Name;

        public string Key { get; }
        public ulong Cas { get; set; }
        public T Content { get; }
        public TransactionDocumentStatus Status { get; set; }
        public TransactionLinks<T> Links { get; }

        public TransactionDocument(IBucket bucket, string key, ulong cas, T content, TransactionDocumentStatus status, TransactionLinks<T> links = null)
        {
            Bucket = bucket;
            Key = key;
            Cas = cas;
            Content = content;
            Status = status;
            Links = links;
        }
    }
}
