using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Cleanup
{
    internal record CleanupRequest(
            string AttemptId,
            string AtrId,
            ICouchbaseCollection AtrCollection,
            List<DocRecord> InsertedIds,
            List<DocRecord> ReplacedIds,
            List<DocRecord> RemovedIds,
            AttemptStates State,
            DateTimeOffset WhenReadyToBeProcessed,
            ConcurrentQueue<Exception> ProcessingErrors,
            JObject? ForwardCompatibility = null);
}
