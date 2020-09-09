using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.Core;
using Couchbase.KeyValue;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;

namespace Couchbase.Transactions
{
    public class TransactionAttempt
    {
        public TimeSpan TimeTaken { get; internal set; }
        public ICouchbaseCollection? AtrCollection { get; internal set; }
        public string? AtrId { get; internal set; }
        public AttemptStates FinalState { get; internal set; }
        public string AttemptId { get; internal set; }
        public IEnumerable<string> StagedInsertedIds { get; internal set; }
        public IEnumerable<string> StagedReplaceIds { get; internal set; }
        public IEnumerable<string> StagedRemoveIds { get; internal set; }
        public Exception? TermindatedByException { get; internal set; }
        public IEnumerable<MutationToken> MutationTokens { get; internal set; }

        public override string ToString()
        {
            const int logDeferCharsToLog = 5; // TODO: move constant to equivalent of LogDefer class in Java code.
            var sb = new StringBuilder();
            sb.Append(nameof(TransactionAttempt)).Append("{id=").Append(AttemptId.SafeSubstring(logDeferCharsToLog));
            sb.Append(",state=").Append(FinalState);
            sb.Append(",atrColl=").Append(AtrCollection?.Name ?? "<none>");
            sb.Append("/atrId=").Append(AtrId ?? "<none>");
            sb.Append("}");
            return sb.ToString();
        }
    }
}
