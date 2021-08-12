using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.DataModel
{
    internal record QueryErrorCause(string? cause, bool? rollback, bool? retry, string? raise);
}
