using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.KeyValue;
using Couchbase.Query;
using Couchbase.Transactions.Cleanup;
using Couchbase.Transactions.Support;

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
