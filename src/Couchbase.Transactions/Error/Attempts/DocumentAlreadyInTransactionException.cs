using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Retry;

namespace Couchbase.Transactions.Error.Attempts
{
    public class DocumentAlreadyInTransactionException : AttemptException, IRetryable
    {
        public TransactionGetResult Doc { get; }

        private DocumentAlreadyInTransactionException(AttemptContext ctx, TransactionGetResult doc, string msg)
            : base(ctx, msg)
        {
            Doc = doc;
        }

        public static DocumentAlreadyInTransactionException Create(AttemptContext ctx, TransactionGetResult doc)
        {
            var msg =
                $"Document {ctx.Redactor.UserData(doc.Id)} is already in a transaction, atr={doc.Links?.AtrIdFull}, attemptId = {doc.Links?.StagedAttemptId ?? "-"}";

            return new DocumentAlreadyInTransactionException(ctx, doc, msg);
        }
    }
}
