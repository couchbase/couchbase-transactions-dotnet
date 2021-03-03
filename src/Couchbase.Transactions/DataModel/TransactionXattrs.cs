using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Transactions.Components;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.DataModel
{
    public class TransactionXattrs
    {
        [JsonProperty("id")]
        public CompositeId? Id { get; set; }

        [JsonProperty("atr")]
        public AtrRef? AtrRef { get; set; }

        [JsonProperty("op")]
        public StagedOperation? Operation { get; set; }

        [JsonProperty("restore")]
        public DocumentMetadata? RestoreMetadata { get; set; }

        [JsonProperty("fc")]
        public JObject? ForwardCompatibility { get; set; }

        internal void ValidateMinimum()
        {
            if (Id?.AttemptId == null
                || Id?.Transactionid == null
                || AtrRef?.Id == null
                || AtrRef?.BucketName == null
                || AtrRef?.CollectionName == null)
            {
                throw new InvalidOperationException("Transaction metadata was in invalid state.");
            }
        }
    }
}
