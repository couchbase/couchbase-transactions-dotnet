using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error
{
    public class TransactionCommitAmbiguousException : TransactionFailedException
    {
        public TransactionCommitAmbiguousException(string message, Exception innerException, TransactionResult result)
            : base(message, innerException, result)
        {
        }
    }
}
