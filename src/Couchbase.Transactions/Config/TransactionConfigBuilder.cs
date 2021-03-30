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


/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2021 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
