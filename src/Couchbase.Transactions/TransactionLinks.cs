namespace Couchbase.Transactions
{
    internal class TransactionLinks<T>
    {
        public string AtrId { get; }
        public string AtrBucketName { get; }
        public string StagedVersion { get; }
        public T StagedContent { get; }

        public TransactionLinks(string atrId, string atrBucketName, string stagedVersion, T stagedContent)
        {
            AtrId = atrId;
            StagedVersion = stagedVersion;
            StagedContent = stagedContent;
            AtrBucketName = atrBucketName;
        }
    }
}