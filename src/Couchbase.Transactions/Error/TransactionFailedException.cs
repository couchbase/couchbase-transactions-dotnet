using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error
{
    public class TransactionFailedException : CouchbaseException
    {
        public TransactionFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
