using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class ActiveTransactionRecordsFullException : AttemptException
    {
        private ActiveTransactionRecordsFullException(AttemptContext ctx, string msg) : base(ctx, msg)
        {
        }
    }
}
