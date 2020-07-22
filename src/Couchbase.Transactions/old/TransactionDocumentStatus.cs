using System.ComponentModel;

namespace Couchbase.Transactions.old
{
    public enum TransactionDocumentStatus
    {
        [Description("NORMAL")]
        Normal,
        
        [Description("IN_COMMITTED")]
        InCommitted,

        [Description("IN_OTHER")]
        InOther,

        [Description("OWN_WRITE")]
        OwnWrite,

        [Description("AMBIGUOUS")]
        Ambiguous
    }
}
