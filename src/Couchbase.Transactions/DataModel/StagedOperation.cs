using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.DataModel
{
    public class StagedOperation
    {
        [JsonProperty("type")]
        public string? Type { get; set; }

        // TODO: while this is part of the data model, the fact that we have to read separately so that we
        // have access to LookupInResult.ContentAs<T>() later means that we're keeping this in memory twice.
        // That could be significant if the document is large.
        [JsonProperty("stgd")]
        public object? StagedDocument { get; set; }

        [JsonProperty("crc32")]
        public string? Crc32 { get; set; }
    }
}
