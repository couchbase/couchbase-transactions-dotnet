using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit.Abstractions;

namespace Couchbase.Transactions.Tests.IntegrationTests
{
    public class TestOutputLogger : ILogger
    {
        private readonly ITestOutputHelper _outputHelper;
        private readonly LogLevel _logLevel;

        public TestOutputLogger(ITestOutputHelper outputHelper, LogLevel logLevel)
        {
            _outputHelper = outputHelper;
            _logLevel = logLevel;
        }
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                try
                {
                    _outputHelper.WriteLine($"{logLevel}: [{eventId}] {formatter(state, exception)}");
                }
                catch
                {
                    // multi-threaded code can cause the test output helper to throw if logged to after the test is finished.
                }
            }
        }

        public bool IsEnabled(LogLevel logLevel) => _logLevel <= logLevel;

        public IDisposable BeginScope<TState>(TState state) => new Mock<IDisposable>().Object;
    }
}
