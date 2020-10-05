using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Couchbase.Transactions.Error.Internal
{
    public class TransactionOperationFailedException : CouchbaseException, IClassifiedTransactionError
    {
        public const TransactionOperationFailedException None = null;

        private static long ExceptionCount = 0;

        private readonly AttemptContext _ctx;

        public long ExceptionNumber { get; }
        public ErrorClass CausingErrorClass { get; }
        public bool AutoRollbackAttempt { get; }
        public bool RetryTransaction { get; }
        public Exception Cause { get; }
        internal FinalError FinalErrorToRaise { get; }

        public enum FinalError
        {
            TransactionFailed = 0,
            TransactionExpired = 1,
            TransactionCommitAmbiguous = 2,

            /**
             * This will currently result in returning success to the application, but unstagingCompleted() will be false.
             */
            TransactionFailedPostCommit = 3
        }

        public TransactionOperationFailedException(
            AttemptContext ctx,
            ErrorClass causingErrorClass,
            bool autoRollbackAttempt,
            bool retryTransaction,
            Exception cause,
            FinalError finalErrorToRaise)
        {
            ExceptionNumber = Interlocked.Increment(ref ExceptionCount);
            _ctx = ctx;
            CausingErrorClass = causingErrorClass;
            AutoRollbackAttempt = autoRollbackAttempt;
            RetryTransaction = retryTransaction;
            Cause = cause;
            FinalErrorToRaise = finalErrorToRaise;
        }
    }
}
