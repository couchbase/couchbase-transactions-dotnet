using System;

namespace Couchbase.Transactions.Error.External
{
    public class TransactionAbortedExternallyException : Exception
    {
        public TransactionAbortedExternallyException()
            : base("This transaction was aborted by another actor, and was unable to commit")
        {
        }
    }
}
