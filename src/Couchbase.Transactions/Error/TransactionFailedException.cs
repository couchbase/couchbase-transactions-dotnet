using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error
{
    public class TransactionFailedException : CouchbaseException
    {
        public TransactionResult Result { get; }

        public TransactionFailedException(string message, Exception innerException, TransactionResult result) : base(message, innerException)
        {
            Result = result;
        }
    }
}
