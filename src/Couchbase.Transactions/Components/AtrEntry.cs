using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Transactions.Support;
using Newtonsoft.Json;

namespace Couchbase.Transactions.Components
{
    internal class AtrEntry
    {
        [JsonProperty(TransactionFields.AtrFieldTransactionId)]
        public string? TransactionId { get; set; }

        [JsonProperty(TransactionFields.AtrFieldStatus)]
        public AttemptStates State { get; set; }

        [JsonProperty(TransactionFields.AtrFieldStartTimestamp)]
        public string? TimestampStartCas { get; set; }

        [JsonIgnore]
        public DateTimeOffset? TimestampStartMsecs => ActiveTransactionRecord.ParseMutationCasField(TimestampStartCas);

        [JsonProperty(TransactionFields.AtrFieldStartCommit)]
        public string? TimestampCommitCas { get; }

        [JsonIgnore]
        public DateTimeOffset? TimestampCommitMsecs => ActiveTransactionRecord.ParseMutationCasField(TimestampCommitCas);

        [JsonProperty(TransactionFields.AtrFieldTimestampComplete)]
        public string? TimestampCompleteCas { get; set; }

        [JsonIgnore]
        public DateTimeOffset? TimestampCompleteMsecs => ActiveTransactionRecord.ParseMutationCasField(TimestampCompleteCas);

        [JsonProperty(TransactionFields.AtrFieldTimestampRollbackStart)]
        public string? TimestampRollBackCas { get; set; }

        [JsonIgnore]
        public DateTimeOffset? TimestampRollBackMsecs => ActiveTransactionRecord.ParseMutationCasField(TimestampRollBackCas);

        [JsonProperty(TransactionFields.AtrFieldTimestampRollbackComplete)]
        public string? TimestampRolledBackCas { get; set; }

        [JsonIgnore]
        public DateTimeOffset? TimestampRolledBackMsecs => ActiveTransactionRecord.ParseMutationCasField(TimestampRolledBackCas);

        [JsonProperty(TransactionFields.AtrFieldExpiresAfterMsecs)]
        public int? ExpiresAfterMsecs { get; set; }

        [JsonProperty(TransactionFields.AtrFieldDocsInserted)]
        public IList<DocRecord> InsertedIds { get; set; }

        [JsonProperty(TransactionFields.AtrFieldDocsReplaced)]
        public IList<DocRecord> ReplacedIds { get; set; }

        [JsonProperty(TransactionFields.AtrFieldDocsRemoved)]
        public IList<DocRecord> RemovedIds { get; set; }

        public ulong? Cas { get; }
    }
}
