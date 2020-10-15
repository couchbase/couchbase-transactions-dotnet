using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Couchbase.Transactions.DataModel
{
    public class CompositeId
    {
        [JsonProperty("txn")]
        public string? Transactionid { get; set; }

        [JsonProperty("atmpt")]
        public string? AttemptId { get; set; }
    }
}
