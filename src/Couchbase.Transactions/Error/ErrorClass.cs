using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Exceptions;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Transactions.Error.Attempts;
using Couchbase.Transactions.Error.Internal;

namespace Couchbase.Transactions.Error
{
    public enum ErrorClass
    {
        Undefined = 0,
        FailTransient = 1,
        FailHard = 2,
        FailOther = 3,
        FailAmbiguous = 4,
        FailDocAlreadyExists = 5,
        FailDocNotFound = 6,
        FailPathAlreadyExists = 7,
        FailPathNotFound = 8,
        FailCasMismatch = 9,
        FailExpiry = 10,
        FailWriteWriteConflict = 11,
        FailAtrFull = 12,
        TransactionOperationFailed = 255
    }

    public static class ErrorClassExtensions
    {
        public static ErrorClass Classify(this Exception ex)
        {
            if (ex is IClassifiedTransactionError classifiedError)
            {
                return classifiedError.CausingErrorClass;
            }

            if (ex is TransactionFailedException)
            {
                return ErrorClass.TransactionOperationFailed;
            }

            if (ex is DocumentAlreadyInTransactionException)
            {
                return ErrorClass.FailWriteWriteConflict;
            }

            if (ex is DocumentNotFoundException)
            {
                return ErrorClass.FailDocNotFound;
            }

            if (ex is DocumentExistsException)
            {
                return ErrorClass.FailDocAlreadyExists;
            }

            if (ex is PathExistsException)
            {
                return ErrorClass.FailPathAlreadyExists;
            }

            if (ex is PathNotFoundException)
            {
                return ErrorClass.FailPathNotFound;
            }

            if (ex is CasMismatchException)
            {
                return ErrorClass.FailCasMismatch;
            }

            if (ex.IsFailTransient())
            {
                return ErrorClass.FailTransient;
            }

            if (ex.IsFailAmbiguous())
            {
                return ErrorClass.FailAmbiguous;
            }

            // ErrorClass.FailHard, from the java code, is handled by IClassifiedTransactionError

            if (ex is AttemptExpiredException)
            {
                return ErrorClass.FailExpiry;
            }

            if (ex is ValueToolargeException)
            {
                return ErrorClass.FailAtrFull;
            }

            return ErrorClass.FailOther;
        }

        internal static bool IsFailTransient(this Exception ex)
        {
            switch (ex)
            {
                case CasMismatchException _:

                // TXNJ-156: With BestEffortRetryStrategy, many errors such as TempFails will now surface as
                // timeouts instead.  This will include AmbiguousTimeoutException - we should already be able to
                // handle ambiguity, as with DurabilityAmbiguousException
                case UnambiguousTimeoutException _:

                // These only included because several tests explicitly throw them as an error-injection.  Those
                // should be changed to return a more correct TimeoutException.
                case TemporaryFailureException _:
                case DurableWriteInProgressException _:
                    return true;
                default:
                    return false;
            }
        }

        internal static bool IsFailAmbiguous(this Exception ex)
        {
            switch (ex)
            {
                case DurabilityAmbiguousException _:
                case AmbiguousTimeoutException _:
                case RequestCanceledException _:
                    return true;

                default:
                    return false;
            }
        }
    }
}
