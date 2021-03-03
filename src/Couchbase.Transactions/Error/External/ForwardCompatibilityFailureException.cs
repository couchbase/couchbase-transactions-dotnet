using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.External
{
    public class ForwardCompatibilityFailureException : Exception
    {
        public ForwardCompatibilityFailureException() : base()
        {
        }

        public ForwardCompatibilityFailureException(string message) : base(message)
        {
        }

        public ForwardCompatibilityFailureException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
