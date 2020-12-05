using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Newtonsoft.Json;

namespace Couchbase.Transactions.DataModel
{
    public class AtrRef
    {
        [JsonProperty("id")]
        public string? Id { get; set; }

        [JsonProperty("bkt")]
        public string? BucketName { get; set; }

        [JsonProperty("scp")]
        public string? ScopeName { get; set; }

        [JsonProperty("coll")]
        public string? CollectionName { get; set; }

        public override string ToString() => $"{BucketName ?? "-"}/{ScopeName ?? "-"}/{CollectionName ?? "-"}/{Id ?? "-"}";
    }
}
