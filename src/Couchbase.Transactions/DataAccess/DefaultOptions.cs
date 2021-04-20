using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Retry;
using Couchbase.KeyValue;

namespace Couchbase.Transactions.DataAccess
{
    internal static class DefaultOptions
    {
        public static IRetryStrategy RetryStrategy = new BestEffortRetryStrategy();

        public static LookupInOptions Defaults(this LookupInOptions opts, TimeSpan? timeout)
        {
            opts = opts.RetryStrategy(RetryStrategy);
            if (timeout.HasValue)
            {
                opts = opts.Timeout(timeout.Value);
            }

            return opts;
        }

        public static MutateInOptions Defaults(this MutateInOptions opts, DurabilityLevel? durability, TimeSpan? timeout)
        {
            opts = new MutateInOptions().RetryStrategy(RetryStrategy);
            if (durability.HasValue)
            {
                opts = opts.Durability(durability.Value);
            }

            if (timeout.HasValue)
            {
                opts = opts.Timeout(timeout.Value);
            }

            return opts;
        }

        public static InsertOptions Defaults(this InsertOptions opts, DurabilityLevel? durability, TimeSpan? timeout)
        {
            opts = new InsertOptions().RetryStrategy(RetryStrategy);
            if (durability.HasValue)
            {
                opts = opts.Durability(durability.Value);
            }

            if (timeout.HasValue)
            {
                opts = opts.Timeout(timeout.Value);
            }

            return opts;
        }

        public static RemoveOptions Defaults(this RemoveOptions opts, DurabilityLevel? durability, TimeSpan? timeout)
        {
            opts = new RemoveOptions().RetryStrategy(RetryStrategy);
            if (durability.HasValue)
            {
                opts = opts.Durability(durability.Value);
            }

            if (timeout.HasValue)
            {
                opts = opts.Timeout(timeout.Value);
            }

            return opts;
        }
    }
}
