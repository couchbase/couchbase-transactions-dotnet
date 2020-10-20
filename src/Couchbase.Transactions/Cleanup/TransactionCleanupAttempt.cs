using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Cleanup
{
    internal readonly struct TransactionCleanupAttempt
    {
        public bool Success { get; }
        public bool IsRegular { get; }
        public string AttemptId { get; }
        public string AtrId { get; }
        public string AtrBucket { get; }
        public CleanupRequest Request { get; }

        internal Exception? FailureReason { get; }

        public TransactionCleanupAttempt(bool success, bool isRegular, CleanupRequest request, Exception failureReason = null)
        {
            Success = success;
            IsRegular = isRegular;
            AttemptId = request.AttemptId;
            AtrId = request.AtrId;
            AtrBucket = request.AtrCollection.Scope.Bucket.Name;
            Request = request;
            FailureReason = failureReason;
        }
    }
}
