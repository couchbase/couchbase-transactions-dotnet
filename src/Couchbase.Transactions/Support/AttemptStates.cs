using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

// ReSharper disable InconsistentNaming

namespace Couchbase.Transactions.Support
{
    public enum AttemptStates
    {
        NOTHING_WRITTEN = 0,

        PENDING = 1,

        ABORTED = 2,

        COMMITTED = 3,

        COMPLETED = 4,

        ROLLED_BACK = 5
    }
}
