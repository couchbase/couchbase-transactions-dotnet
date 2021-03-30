using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.KeyValue;
using Couchbase.Query;
using Couchbase.Transactions.Cleanup;
using Couchbase.Transactions.Support;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions.Config
{
    public class TransactionConfig
    {
        public const DurabilityLevel DefaultDurabilityLevel = DurabilityLevel.Majority;
        public const int DefaultExpirationMilliseconds = 15_000;
        public const int DefaultCleanupWindowMilliseconds = 60_000;
        public const bool DefaultCleanupLostAttempts = true;
        public const bool DefaultCleanupClientAttempts = true;
        public const Severity DefaultLogOnFailure = Severity.Error;

        public TimeSpan ExpirationTime { get; internal set; }
        public bool CleanupLostAttempts { get; internal set; }
        public bool CleanupClientAttempts { get; internal set; }
        public TimeSpan CleanupWindow { get; internal set; }
        public bool LogDirectly => LogDirectlyLevel.HasValue;
        public Severity? LogDirectlyLevel { get; internal set; }
        public bool LogOnFailure => LogOnFailureLevel.HasValue;
        public Severity? LogOnFailureLevel { get; internal set; }
        public TimeSpan? KeyValueTimeout { get; internal set; }
        public DurabilityLevel DurabilityLevel { get; internal set; }
        public ILoggerFactory? LoggerFactory { get; internal set; }

        internal TransactionConfig(
            DurabilityLevel durabilityLevel = DefaultDurabilityLevel,
            Severity? logDirectly = null,
            Severity? logOnFailure = DefaultLogOnFailure,
            TimeSpan? expirationTime = null,
            TimeSpan? cleanupWindow = null,
            TimeSpan? keyValueTimeout = null,
            bool cleanupClientAttempts = DefaultCleanupClientAttempts,
            bool cleanupLostAttempts = DefaultCleanupLostAttempts
        )
        {
            ExpirationTime = expirationTime ?? TimeSpan.FromMilliseconds(DefaultExpirationMilliseconds);
            CleanupLostAttempts = cleanupLostAttempts;
            CleanupClientAttempts = cleanupClientAttempts;
            CleanupWindow = cleanupWindow ?? TimeSpan.FromMilliseconds(DefaultCleanupWindowMilliseconds);
            LogDirectlyLevel = logDirectly;
            LogOnFailureLevel = logOnFailure;
            KeyValueTimeout = keyValueTimeout;
            DurabilityLevel = durabilityLevel;
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
