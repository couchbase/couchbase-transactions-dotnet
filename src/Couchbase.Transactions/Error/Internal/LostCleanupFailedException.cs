using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Internal
{
    internal class LostCleanupFailedException : Exception, IClassifiedTransactionError
    {
        public LostCleanupFailedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public LostCleanupFailedException(string message) : base(message)
        {
        }

        public ErrorClass CausingErrorClass { get; init; }
    }
}
