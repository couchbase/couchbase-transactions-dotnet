using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Error
{
    internal class ErrorBuilder
    {
        public const ErrorBuilder None = null;
        private readonly AttemptContext _ctx;
        private readonly ErrorClass _causingErrorClass;
        private TransactionOperationFailedException.FinalError _toRaise = TransactionOperationFailedException.FinalError.TransactionFailed;
        private bool _rollbackAttempt;
        private bool _retryTransaction;
        private Exception _cause = new Exception("generic exception cause");

        private ErrorBuilder(AttemptContext ctx, ErrorClass causingErrorClass)
        {
            _ctx = ctx;
            _causingErrorClass = causingErrorClass;
        }

        public static ErrorBuilder CreateError(AttemptContext ctx, ErrorClass causingErrorClass, Exception? causingException = null)
        {
            var builder = new ErrorBuilder(ctx, causingErrorClass);
            if (causingException != null)
            {
                builder.Cause(causingException);
            }

            return builder;
        }

        public ErrorBuilder RaiseException(TransactionOperationFailedException.FinalError finalErrorToRaise)
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

        public ErrorBuilder Cause(Exception cause)
        {
            if (cause.StackTrace != null)
            {
                _cause = cause;
                return this;
            }

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

        public TransactionOperationFailedException Build() => new TransactionOperationFailedException(_ctx, _causingErrorClass, _rollbackAttempt, _retryTransaction, _cause, _toRaise);
    }
}
