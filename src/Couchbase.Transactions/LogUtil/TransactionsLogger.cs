﻿using System;
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