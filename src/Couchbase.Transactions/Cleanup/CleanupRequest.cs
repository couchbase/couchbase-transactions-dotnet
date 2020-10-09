using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Support;

namespace Couchbase.Transactions.Cleanup
{
    internal class CleanupRequest
    {
        public CleanupRequest(string attemptId, string atrId, ICouchbaseCollection atrCollection, List<DocRecord> insertedIds, List<DocRecord> replacedIds, List<DocRecord> removedIds, AttemptStates state, DateTimeOffset whenReadyToBeProcessed)
        {
            AttemptId = attemptId;
            AtrId = atrId;
            AtrCollection = atrCollection;
            InsertedIds = insertedIds;
            ReplacedIds = replacedIds;
            RemovedIds = removedIds;
            State = state;
            WhenReadyToBeProcessed = whenReadyToBeProcessed;
        }

        public string AttemptId { get; }
        public string AtrId { get; }
        public ICouchbaseCollection AtrCollection { get; }
        public List<DocRecord> InsertedIds { get; }
        public List<DocRecord> ReplacedIds { get; }
        public List<DocRecord> RemovedIds { get; }

        public AttemptStates State { get; }

        public DateTimeOffset WhenReadyToBeProcessed { get; internal set; }

        // TODO: ForwardCompatibility option

        public ConcurrentQueue<Exception> ProcessingErrors { get; } = new ConcurrentQueue<Exception>();
    }
}
