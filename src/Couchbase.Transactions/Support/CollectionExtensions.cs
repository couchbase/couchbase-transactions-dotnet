using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;

namespace Couchbase.Transactions.Support
{
    internal static class CollectionExtensions
    {
        public static RemoveOptions Timeout(this RemoveOptions opts, TimeSpan? timeout)
        {
            if (timeout.HasValue)
            {
                opts.Timeout(timeout.Value);
            }

            return opts;
        }

        public static MutateInOptions Timeout(this MutateInOptions opts, TimeSpan? timeout)
        {
            if (timeout.HasValue)
            {
                opts.Timeout(timeout.Value);
            }

            return opts;
        }

        public static LookupInOptions Timeout(this LookupInOptions opts, TimeSpan? timeout)
        {
            if (timeout.HasValue)
            {
                opts.Timeout(timeout.Value);
            }

            return opts;
        }
    }
}
