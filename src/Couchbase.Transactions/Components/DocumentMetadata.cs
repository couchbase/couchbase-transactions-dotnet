using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Couchbase.Transactions.Components
{
    public class DocumentMetadata
    {
        [JsonProperty("CAS")]
        public string? Cas { get; internal set; }

        [JsonProperty("revid")]
        public string? RevId { get; internal set; }

        [JsonProperty("exptime")]
        public ulong? ExpTime { get; internal set; }

        [JsonProperty("value_crc32c")]
        public string? Crc32c { get; internal set; }
    }
}
