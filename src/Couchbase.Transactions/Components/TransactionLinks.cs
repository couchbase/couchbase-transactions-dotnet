using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Transactions.ActiveTransactionRecords;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Components
{
    public class TransactionLinks
    {
        public string? StagedContent { get; }
        public string? AtrId { get; }
        public string? AtrBucketName { get; }
        public string? AtrScopeName { get; }
        public string? AtrCollectionName { get; }
        public string? StagedTransactionId { get; }
        public string? StagedAttemptId { get; }
        public string? CasPreTxn { get; }
        public string? RevIdPreTxn { get; }
        public ulong? ExptimePreTxn { get; }
        public string? Op { get; }

        public string AtrIdFull => $"{AtrBucketName ?? "-"}/{AtrScopeName ?? "-"}/{AtrCollectionName ?? "-"}/{AtrId ?? "-"}";

        public bool IsDocumentInTransaction => AtrId != null;

        public bool IsDocumentBeingRemoved =>
            StagedContent?.Equals(ActiveTransactionRecord.Remove, StringComparison.Ordinal) == true;

        public bool HasStagedWrite => StagedAttemptId != null;

        public TransactionLinks(
            string? stagedContent,
            string? atrId,
            string? atrBucketName,
            string? atrScopeName,
            string? atrCollectionName,
            string? stagedTransactionId,
            string? stagedAttemptId,
            string? casPreTxn,
            string? revIdPreTxn,
            ulong? exptimePreTxn,
            string? op)
        {
            StagedContent = stagedContent ?? throw new ArgumentNullException(nameof(stagedContent));
            AtrId = atrId ?? throw new ArgumentNullException(nameof(atrId));
            AtrBucketName = atrBucketName ?? throw new ArgumentNullException(nameof(atrBucketName));
            AtrScopeName = atrScopeName ?? throw new ArgumentNullException(nameof(atrScopeName));
            AtrCollectionName = atrCollectionName ?? throw new ArgumentNullException(nameof(atrCollectionName));
            StagedTransactionId = stagedTransactionId ?? throw new ArgumentNullException(nameof(stagedTransactionId));
            StagedAttemptId = stagedAttemptId ?? throw new ArgumentNullException(nameof(stagedAttemptId));
            CasPreTxn = casPreTxn;
            RevIdPreTxn = revIdPreTxn;
            ExptimePreTxn = exptimePreTxn;
            Op = op ?? throw new ArgumentNullException(nameof(op));
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder("TransactionLinks{");
            sb.Append("atr=").Append(AtrId ?? "none");
            sb.Append(",atrBkt=").Append(AtrBucketName ?? "none");
            sb.Append(",atrColl=").Append(AtrCollectionName ?? "none");
            sb.Append(",txnId=").Append(StagedTransactionId ?? "none");
            sb.Append(",attemptId=").Append(StagedAttemptId ?? "none");
            if (StagedContent != null)
            {
                if (StagedContent.Length <= 20)
                {
                    sb.Append(",content=").Append(StagedContent);
                }
                else
                {
                    sb.Append(",content=").Append(StagedContent.Length).Append("chars");
                }
            }
            sb.Append(",op=").Append(Op ?? "none");
            sb.Append(",restore={");
            sb.Append(CasPreTxn ?? "none");
            sb.Append(',');
            sb.Append(RevIdPreTxn ?? "none");
            sb.Append(',');
            sb.Append(ExptimePreTxn?.ToString() ?? "-1");
            sb.Append("}}");
            return sb.ToString();
        }
    }
}
