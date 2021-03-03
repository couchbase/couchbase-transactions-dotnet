using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.External
{
    public class ForwardCompatibilityFailureRequiresRetryException : Exception
    {
        public ForwardCompatibilityFailureRequiresRetryException() : base()
        {
        }

        public ForwardCompatibilityFailureRequiresRetryException(string message) : base(message)
        {
        }

        public ForwardCompatibilityFailureRequiresRetryException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
