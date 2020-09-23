using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Couchbase.Transactions.Error.external
{
    /**
     * A previous operation in the application's lambda failed, and so the currently-attempt operation is also not
     * permitted to proceed.
     *
     * This is most likely thrown in one of these two scenarios:
     *
     * 1. The application is performing multiple operations in parallel and one of them has failed.  For performance it is
     *    best to fail all other operations immediately (the transaction is not going to commit anyway), so can get to the
     *    fail and possibly retry point as soon as possible.
     * 2. The application is erroneously catching and not propagating exceptions in the lambda.
     */
    public class PreviousOperationFailedException : Exception
    {
        private IEnumerable<Exception> Causes { get; } = Enumerable.Empty<Exception>();

        public PreviousOperationFailedException()
        {
        }

        public PreviousOperationFailedException(IEnumerable<Exception> causes)
        {
            Causes = causes;
        }
    }
}
