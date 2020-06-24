using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;

namespace Couchbase.Transactions.Config
{
    public class PerTransactionConfigBuilder
    {
        private readonly PerTransactionConfig _config;

        private PerTransactionConfigBuilder()
        {
            _config = new PerTransactionConfig();
        }
        
        public static PerTransactionConfigBuilder Create() => new PerTransactionConfigBuilder();

        public PerTransactionConfigBuilder DurabilityLevel(DurabilityLevel durabilityLevel)
        {
            _config.DurabilityLevel = durabilityLevel;
            return this;
        }
    }
}
