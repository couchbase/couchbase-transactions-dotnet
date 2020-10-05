using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class ActiveTransactionRecordsFullException : AttemptException
    {
        internal ActiveTransactionRecordsFullException(AttemptContext ctx, string? msg = null) : base(ctx, msg ?? "Active Transaction Record full")
        {
        }
    }
}
