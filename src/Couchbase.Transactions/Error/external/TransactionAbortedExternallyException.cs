using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.external
{
    public class TransactionAbortedExternallyException : Exception
    {
        public TransactionAbortedExternallyException()
            : base("This transaction was aborted by another actor, and was unable to commit")
        {
        }
    }
}
