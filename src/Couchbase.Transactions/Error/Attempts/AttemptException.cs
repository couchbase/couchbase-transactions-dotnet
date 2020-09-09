using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Attempts
{
    public class AttemptException : CouchbaseException
    {
        private AttemptContext _ctx;
        private string _msg;

        public AttemptException(AttemptContext ctx, string msg)
        {
            _ctx = ctx;
            _msg = msg;
        }
    }
}
