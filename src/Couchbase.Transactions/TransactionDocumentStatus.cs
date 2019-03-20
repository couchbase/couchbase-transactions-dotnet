namespace Couchbase.Transactions
{
    public enum TransactionDocumentStatus
    {
        Normal,
        InCommitted,
        InOther,
        OwnWrite,
        Ambiguous
    }
}