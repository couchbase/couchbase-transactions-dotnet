﻿using System;
using Couchbase.Logging;

namespace Couchbase.Transactions.Config
{
    public class TransactionConfigBuilder
    {
        private int _maxAttempts = 100;
        private TimeSpan _expiration = TimeSpan.FromSeconds(15);
        private TimeSpan _keyValueTimeout = TimeSpan.FromMilliseconds(2500);
        private PersistTo _persistTo = Couchbase.PersistTo.Zero;
        private ReplicateTo _replicateTo = Couchbase.ReplicateTo.One;
        private LogLevel _logLevel = Logging.LogLevel.Off;

        private bool _cleanupLostAttempts = true;
        private bool _cleanupClientAttempts = true;
        private TimeSpan _cleanupWindow = TimeSpan.FromMinutes(2);
        private TimeSpan _cleanupStatsInterval = TimeSpan.FromMinutes(1);
        private LogLevel _cleanupLogLevel = Logging.LogLevel.Off;

        private bool _logOnFailure;
        private LogLevel _onFailureLogLevel = Logging.LogLevel.Debug;
        private LogLevel _cleanupOnFailureLogLevel = Logging.LogLevel.Info;

        public TransactionConfig Build()
        {
            return new TransactionConfig(
                _maxAttempts,
                _expiration,
                _keyValueTimeout,
                _persistTo,
                _replicateTo,
                _logLevel,
                _cleanupLostAttempts,
                _cleanupClientAttempts,
                _cleanupWindow,
                _cleanupStatsInterval,
                _cleanupLogLevel,
                _logOnFailure,
                _onFailureLogLevel,
                _cleanupOnFailureLogLevel
            );
        }

        public TransactionConfigBuilder MaxAttempts(int maxAttempts)
        {
            _maxAttempts = maxAttempts;
            return this;
        }

        public TransactionConfigBuilder Expiration(TimeSpan expiration)
        {
            _expiration = expiration;
            return this;
        }

        public TransactionConfigBuilder KeyValueTimeout(TimeSpan keyValueTimeout)
        {
            _keyValueTimeout = keyValueTimeout;
            return this;
        }

        public TransactionConfigBuilder PersistTo(PersistTo persistTo)
        {
            _persistTo = persistTo;
            return this;
        }

        public TransactionConfigBuilder ReplicateTo(ReplicateTo replicateTo)
        {
            _replicateTo = replicateTo;
            return this;
        }

        public TransactionConfigBuilder LogLevel(LogLevel logLevel)
        {
            _logLevel = logLevel;
            return this;
        }

        public TransactionConfigBuilder CleanupLostAttempts(bool cleanupLostAttempts)
        {
            _cleanupLostAttempts = cleanupLostAttempts;
            return this;
        }

        public TransactionConfigBuilder CleanupClientAttempts(bool cleanupClientAttempts)
        {
            _cleanupClientAttempts = cleanupClientAttempts;
            return this;
        }

        public TransactionConfigBuilder CleanupWindow(TimeSpan cleanupInterval)
        {
            _cleanupWindow = cleanupInterval;
            return this;
        }

        public TransactionConfigBuilder CleanupStatsInterval(TimeSpan cleanupStatsInterval)
        {
            _cleanupStatsInterval = cleanupStatsInterval;
            return this;
        }

        public TransactionConfigBuilder CleanupLogLevel(LogLevel cleanupLogLevel)
        {
            _cleanupLogLevel = cleanupLogLevel;
            return this;
        }

        public TransactionConfigBuilder LogOnFailure(bool logOnFailure)
        {
            _logOnFailure = logOnFailure;
            return this;
        }

        public TransactionConfigBuilder OnFailureLogLevel(LogLevel onFailureLogLevel)
        {
            _onFailureLogLevel = onFailureLogLevel;
            return this;
        }

        public TransactionConfigBuilder CleanupOnFailureLogLevel(LogLevel cleanupOnFailureLogLevel)
        {
            _cleanupOnFailureLogLevel = cleanupOnFailureLogLevel;
            return this;
        }
    }
}
