using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Cleanup
{
    internal record TransactionCleanupAttempt(
        bool Success,
        bool IsRegular,
        string AttemptId,
        string AtrId,
        string AtrBucketName,
        string AtrCollectionName,
        string AtrScopeName,
        CleanupRequest Request,
        Exception? FailureReason);
}
