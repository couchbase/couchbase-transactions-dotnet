using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Diagnostics.Tracing;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Components
{
    [Obsolete("Use " + nameof(AtrEntry) + " instead.")]
    internal class ActiveTransactionRecord
    {
        [JsonProperty("attempts")]
        public Dictionary<string, AtrEntry> Attempts { get; set; } = new Dictionary<string, AtrEntry>();

        public static AtrEntry? CreateFrom(string bucketName, string atrId, JToken entry, string attemptId, string transactionId, ulong? cas) => throw new NotSupportedException();

        internal static DateTimeOffset? ParseMutationCasField(string? casString) => throw new NotSupportedException();
    }
}
