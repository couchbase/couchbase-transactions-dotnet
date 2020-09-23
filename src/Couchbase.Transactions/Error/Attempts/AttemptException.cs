using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class AttemptException : CouchbaseException
    {
        private AttemptContext _ctx;

        public AttemptException(AttemptContext ctx, string msg)
            : base(msg)
        {
            _ctx = ctx;
        }

        public AttemptException(AttemptContext ctx, string msg, Exception innerException)
            : base(msg, innerException)
        {
            _ctx = ctx;
        }
    }
}
