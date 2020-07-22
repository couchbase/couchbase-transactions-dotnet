using System.ComponentModel;

namespace Couchbase.Transactions.old
{
    public enum AttemptState
    {
        [Description("NOT_STARTED")]
        NotStarted,

        [Description("PENDING")]
        Pending,

        [Description("ABORTED")]
        Aborted,

        [Description("COMMITTED")]
        Committed,

        [Description("COMPLETED")]
        Completed,

        [Description("ROLLED_BACK")]
        RolledBack
    }
}
