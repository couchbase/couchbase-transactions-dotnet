using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Couchbase.Transactions
{
    public enum TransactionJsonDocumentStatus
    {
        /// <summary>
        /// The fetched document was not involved in a transaction.
        /// </summary>
        [Description("NORMAL")]
        Normal = 0,

        /// <summary>
        /// On fetch, the document was found to have staged data from a now-committed transaction, so the staged data has
        /// been returned.
        /// </summary>
        [Description("IN_TXN_COMMITTED")]
        InTxnCommitted = 1,

        /// <summary>
        /// On fetch, the document was found to have staged data from a non-committed transaction, so the document's content
        /// has been returned rather than the staged content.
        /// </summary>
        [Description("IN_TXN_OTHER")]
        InTxnOther = 2,

        /// <summary>
        /// The document has staged data from this transaction.  To support 'read your own writes', the staged data is
        /// returned.
        /// </summary>
        [Description("OWN_WRITE")]
        OwnWrite = 3,

        /// <summary>
        /// Since 1.0.1, this is no longer returned.
        /// </summary>
        [Description("AMBIGUOUS")]
        Ambiguous = 4
    }
}
