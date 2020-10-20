using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Cleanup
{
    internal class CleanupFailedException : Exception
    {
        public CleanupFailedException(Exception cause) : base("Transaction cleanup attempt failed.", cause)
        {

        }
    }
}
