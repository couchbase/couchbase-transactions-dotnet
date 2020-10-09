using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Tests.IntegrationTests.Errors
{
    /// <summary>
    /// An exception class that should not raise out of Transactions.Run(), once thrown.
    /// </summary>
    internal class InternalIntegrationTestException : Exception, IClassifiedTransactionError
    {
        public ErrorClass CausingErrorClass { get; set; } = ErrorClass.FailOther;
    }
}
