using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions.LogUtil
{
    internal class TransactionsLoggerFactory : ILoggerFactory
    {
        private readonly ILoggerFactory _otherLoggerFactory;
        private readonly TransactionContext _overallContext;

        public TransactionsLoggerFactory(ILoggerFactory otherLoggerFactory, TransactionContext overallContext)
        {
            _otherLoggerFactory = otherLoggerFactory;
            _overallContext = overallContext;
        }

        public void AddProvider(ILoggerProvider provider)
        {
            _otherLoggerFactory.AddProvider(provider);
        }

        public ILogger CreateLogger(string categoryName) => new TransactionsLogger(_otherLoggerFactory.CreateLogger(categoryName), _overallContext);

        public void Dispose()
        {
            _otherLoggerFactory.Dispose();
        }
    }
}
