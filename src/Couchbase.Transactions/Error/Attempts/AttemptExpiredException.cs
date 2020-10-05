using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class AttemptExpiredException : AttemptException
    {
        public AttemptExpiredException(AttemptContext ctx, string? msg = null)
            : base(ctx, msg ?? "Attempt Expired")
        {
        }

        public AttemptExpiredException(AttemptContext ctx, string msg, Exception innerException)
            : base(ctx, msg, innerException)
        {
        }
    }
}
