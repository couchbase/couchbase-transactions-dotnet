using System;
using System.Collections.Generic;
using Couchbase.Core;

namespace Couchbase.Transactions.old
{
    public interface ITransactionAttempt
    {
        string AttemptId { get; }
        TimeSpan Duration { get; }
        IBucket AtrBucket { get; }
        string AtrId{ get; }
        AttemptState FinalState { get; }
        IReadOnlyCollection<string> StagedInsertIds { get; }
        IReadOnlyCollection<string> StagedReplaceIds { get; }
        IReadOnlyCollection<string> StagedRemoveIds { get; }
        Exception TerminatedByException { get; }
    }
}
