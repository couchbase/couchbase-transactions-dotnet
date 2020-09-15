using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Error.Internal
{
    public interface IClassifiedTransactionError
    {
        ErrorClass CausingErrorClass { get; }
    }
}
