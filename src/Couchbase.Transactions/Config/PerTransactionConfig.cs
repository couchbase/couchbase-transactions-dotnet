using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;

namespace Couchbase.Transactions.Config
{
    public class PerTransactionConfig
    {
        internal PerTransactionConfig()
        { }

        public DurabilityLevel? DurabilityLevel { get; internal set; }
    }
}
