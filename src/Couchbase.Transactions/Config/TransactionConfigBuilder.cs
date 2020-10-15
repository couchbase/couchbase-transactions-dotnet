using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.IO.Operations;
using Couchbase.KeyValue;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions.Config
{
    public class TransactionConfigBuilder
    {
        private readonly TransactionConfig _config;

        private TransactionConfigBuilder()
        {
            _config = new TransactionConfig();
        }
        public static TransactionConfigBuilder Create() => new TransactionConfigBuilder();

        public TransactionConfigBuilder ExpirationTime(TimeSpan expirationTime)
        {
            _config.ExpirationTime = expirationTime;
            return this;
        }

        public TransactionConfigBuilder DurabilityLevel(DurabilityLevel durabilityLevel)
        {
            _config.DurabilityLevel = durabilityLevel;
            return this;
        }

        public TransactionConfigBuilder KeyValueTimeout(TimeSpan keyValueTimeout)
        {
            _config.KeyValueTimeout = keyValueTimeout;
            return this;
        }

        public TransactionConfigBuilder CleanupWindow(TimeSpan cleanupWindow)
        {
            _config.CleanupWindow = cleanupWindow;
            return this;
        }

        public TransactionConfigBuilder CleanupClientAttempts(bool cleanupClientAttempts)
        {
            _config.CleanupClientAttempts = cleanupClientAttempts;
            return this;
        }

        public TransactionConfigBuilder CleanupLostAttempts(bool cleanupLostAttempts)
        {
            _config.CleanupLostAttempts = cleanupLostAttempts;
            return this;
        }

        public TransactionConfig Build() => _config;

        public TransactionConfigBuilder LoggerFactory(ILoggerFactory loggerFactory)
        {
            _config.LoggerFactory = loggerFactory;
            return this;
        }
    }
}
