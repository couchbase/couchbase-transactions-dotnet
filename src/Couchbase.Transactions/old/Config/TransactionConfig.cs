using System;
using Couchbase.Logging;

namespace Couchbase.Transactions.old.Config
{
    public struct TransactionConfig
    {
        public int MaxAttempts { get; }
        public TimeSpan Expiration { get; }
        public TimeSpan KeyValueTimeout { get; }
        public PersistTo PersistTo { get; }
        public ReplicateTo ReplicateTo { get; }
        public LogLevel LogLevel { get; }

        public bool CleanupLostAttempts { get; }
        public bool CleanupClientAttempts { get; }
        public TimeSpan CleanupWindow { get; }
        public TimeSpan CleanupStatsInterval { get; }
        public LogLevel CleanupLogLevel { get; }

        public bool LogOnFailure { get; }
        public LogLevel LogOnFailureLogLevel { get; }
        public LogLevel CleanupOnFailureLogLevel { get; }

        public TransactionConfig(
            int maxAttempts, TimeSpan expiration, TimeSpan keyValueTimeout, PersistTo persistTo, ReplicateTo replicateTo, LogLevel logLevel,
            bool cleanupLostAttempts, bool cleanupClientAttempts, TimeSpan cleanupWindow, TimeSpan cleanupStatsInterval,
            LogLevel cleanupLogLevel, bool logOnFailure, LogLevel logOnFailureLogLevel, LogLevel cleanupOnFailureLogLevel)
        {
            MaxAttempts = maxAttempts;
            Expiration = expiration;
            KeyValueTimeout = keyValueTimeout;
            PersistTo = persistTo;
            ReplicateTo = replicateTo;
            LogLevel = logLevel;

            CleanupLostAttempts = cleanupLostAttempts;
            CleanupClientAttempts = cleanupClientAttempts;
            CleanupWindow = cleanupWindow;
            CleanupStatsInterval = cleanupStatsInterval;
            CleanupLogLevel = cleanupLogLevel;

            LogOnFailure = logOnFailure;
            LogOnFailureLogLevel = logOnFailureLogLevel;
            CleanupOnFailureLogLevel = cleanupOnFailureLogLevel;
        }
    }
}
