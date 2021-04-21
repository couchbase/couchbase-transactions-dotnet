using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions.LogUtil
{
    /// <summary>
    /// A logger that records results in-memory.
    /// </summary>
    public class TransactionsLogger : ILogger
    {
        private readonly ILogger _otherLogger;
        private readonly TransactionContext _overallContext;

        internal TransactionsLogger(ILogger otherLogger, TransactionContext overallContext)
        {
            _otherLogger = otherLogger;
            _overallContext = overallContext;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return _otherLogger.BeginScope(state);
        }

        public bool IsEnabled(LogLevel logLevel) => _otherLogger.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            _otherLogger.Log(logLevel, eventId, state, exception, formatter);
            if (IsEnabled(logLevel))
            {
                _overallContext.AddLog($"[{DateTimeOffset.UtcNow}] [{logLevel}] {formatter(state, exception)}");
            }
        }
    }
}
