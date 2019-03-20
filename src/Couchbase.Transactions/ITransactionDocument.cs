namespace Couchbase.Transactions
{
    public interface ITransactionDocument<out T>
    {
        string Key { get; }
        ulong Cas { get; }
        T Content { get; }
        TransactionDocumentStatus Status { get; }
    }
}
