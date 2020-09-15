using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class AttemptExpiredException : AttemptException
    {
        public AttemptExpiredException(AttemptContext ctx, string msg) : base(ctx, msg)
        {
        }
    }
}
