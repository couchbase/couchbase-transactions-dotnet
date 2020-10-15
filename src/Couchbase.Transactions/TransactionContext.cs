using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Transactions.Config;

namespace Couchbase.Transactions
{
    internal class TransactionContext
    {
        public string TransactionId { get; }
        public DateTimeOffset StartTime { get; }
        public TransactionConfig Config { get; }
        public PerTransactionConfig PerConfig { get; }

        public DateTimeOffset AbsoluteExpiration => StartTime + Config.ExpirationTime;
        public bool IsExpired => AbsoluteExpiration <= DateTimeOffset.UtcNow;

        public TimeSpan RemainingUntilExpiration => AbsoluteExpiration - DateTimeOffset.UtcNow;

        public TransactionContext(
            [NotNull] string transactionId,
            DateTimeOffset startTime,
            [NotNull] TransactionConfig config,
            [MaybeNull] PerTransactionConfig perConfig)
        {
            TransactionId = transactionId;
            StartTime = startTime;
            Config = config;
            PerConfig = perConfig ?? new PerTransactionConfig();
        }
    }
}
