using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Internal
{
    internal class ErrorWrapperException : CouchbaseException
    {
        private readonly AttemptContext _ctx;
        private readonly ErrorClass _causingErrorClass;
        private readonly bool _autoRollbackAttempt;
        private readonly bool _retryTransaction;
        private readonly Exception _cause;
        private readonly FinalErrorToRaise _finalErrorToRaise;

        internal enum FinalErrorToRaise
        {
            TransactionFailed = 0,
            TransactionExpired = 1,
            TransactionCommitAmbiguous = 2,

            /**
             * This will currently result in returning success to the application, but unstagingCompleted() will be false.
             */
            TransactionFailedPostCommit = 3
        }

        public ErrorWrapperException(
            AttemptContext ctx, 
            ErrorClass causingErrorClass, 
            bool autoRollbackAttempt,
            bool retryTransaction, 
            Exception cause, 
            FinalErrorToRaise finalErrorToRaise)
        {
            _ctx = ctx;
            _causingErrorClass = causingErrorClass;
            _autoRollbackAttempt = autoRollbackAttempt;
            _retryTransaction = retryTransaction;
            _cause = cause;
            _finalErrorToRaise = finalErrorToRaise;
        }
    }
}
