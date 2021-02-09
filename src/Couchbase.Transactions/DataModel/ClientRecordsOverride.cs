using System;
using Couchbase.Transactions.Cleanup.LostTransactions;
using Newtonsoft.Json;

namespace Couchbase.Transactions.DataModel
{
    /// <summary>
    /// A model class for JSON serialization/deserialization of the overrides section of the ClientRecord.
    /// </summary>
    internal record ClientRecordsOverride
    {
        public const string FIELD_OVERRIDE_ENABLED = "enabled";
        public const string FIELD_OVERRIDE_EXPIRES = "expires";

        [JsonProperty(FIELD_OVERRIDE_ENABLED)]
        public bool Enabled { get; init; }

        [JsonProperty(FIELD_OVERRIDE_EXPIRES)]
        public long ExpiresUnixNanos { get; init; }

        [JsonIgnore()]
        public DateTimeOffset Expires => DateTimeOffset.FromUnixTimeMilliseconds(ExpiresUnixNanos / ClientRecordDetails.NanosecondsPerMillisecond);
    }
}
