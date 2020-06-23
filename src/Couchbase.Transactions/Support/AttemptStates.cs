using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Couchbase.Transactions.Support
{
    public enum AttemptStates
    {
        [Description("NOT_STARTED")]
        NotStarted = 0,

        [Description("PENDING")]
        Pending = 1,

        [Description("ABORTED")]
        Aborted = 2,

        [Description("COMMITTED")]
        Committed = 3,

        [Description("COMPLETED")]
        Completed = 4,

        [Description("ROLLED_BACK")]
        RolledBack = 5
    }
}
