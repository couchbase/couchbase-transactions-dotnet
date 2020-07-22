using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Retry;

namespace Couchbase.Transactions.Error.Attempts
{
    public class DocumentAlreadyInTransactionException : AttemptException, IRetryable
    {
    }
}
