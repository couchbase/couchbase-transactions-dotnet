using System;
using System.Threading;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Error.External
{
    public class TransactionOperationFailedException : CouchbaseException, IClassifiedTransactionError
    {
        public const TransactionOperationFailedException None = null;

        private static long ExceptionCount = 0;

        public long ExceptionNumber { get; }
        public ErrorClass CausingErrorClass { get; }
        public bool AutoRollbackAttempt { get; }
        public bool RetryTransaction { get; }
        public Exception Cause { get; }
        public FinalError FinalErrorToRaise { get; }

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
            ErrorClass causingErrorClass,
            bool autoRollbackAttempt,
            bool retryTransaction,
            Exception cause,
            FinalError finalErrorToRaise)
        {
            ExceptionNumber = Interlocked.Increment(ref ExceptionCount);
            CausingErrorClass = causingErrorClass;
            AutoRollbackAttempt = autoRollbackAttempt;
            RetryTransaction = retryTransaction;
            Cause = cause;
            FinalErrorToRaise = finalErrorToRaise;
        }
    }
}
