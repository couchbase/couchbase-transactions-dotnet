using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Error
{
    internal class ErrorBuilder
    {
        private readonly AttemptContext _ctx;
        private readonly ErrorClass _causingErrorClass;
        private ErrorWrapperException.FinalErrorToRaise _toRaise = ErrorWrapperException.FinalErrorToRaise.TransactionFailed;
        private bool _rollbackAttempt;
        private bool _retryTransaction;
        private Exception _cause;

        private ErrorBuilder(AttemptContext ctx, ErrorClass causingErrorClass)
        {
            _ctx = ctx;
            _causingErrorClass = causingErrorClass;
        }

        public static ErrorBuilder CreateError(AttemptContext ctx, ErrorClass causingErrorClass)
        {
            return new ErrorBuilder(ctx, causingErrorClass);
        }

        public ErrorBuilder RaiseException(ErrorWrapperException.FinalErrorToRaise finalErrorToRaise)
        {
            _toRaise = finalErrorToRaise;
            return this;
        }

        public ErrorBuilder DoNotRollbackAttempt()
        {
            _rollbackAttempt = false;
            return this;
        }

        public ErrorBuilder RetryTransaction()
        {
            _retryTransaction = true;
            return this;
        }

        public ErrorBuilder Cause([NotNull] Exception cause)
        {
            // capture the stack trace
            try
            {
                throw cause;
            }
            catch (Exception causeWithStackTrace)
            {
                _cause = causeWithStackTrace;
            }

            return this;
        }

        public ErrorWrapperException Build() => new ErrorWrapperException(_ctx, _causingErrorClass, _rollbackAttempt, _retryTransaction, _cause, _toRaise);
    }
}
