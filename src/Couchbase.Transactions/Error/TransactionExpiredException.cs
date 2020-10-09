using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Error
{
    public class TransactionExpiredException : TransactionFailedException, IClassifiedTransactionError
    {
        public TransactionExpiredException(string message, Exception innerException, TransactionResult result) : base(message, innerException, result)
        {
        }

        public ErrorClass CausingErrorClass => ErrorClass.FailExpiry;
    }
}
