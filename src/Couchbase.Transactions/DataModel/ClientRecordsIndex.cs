using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace Couchbase.Transactions.DataModel
{
    /// <summary>
    /// A model class for JSON serialization/deserialization of the root Client Records object.
    /// </summary>
    internal record ClientRecordsIndex
    {
        public const string CLIENT_RECORD_DOC_ID = "_txn:client-record";
        public const string VBUCKET_HLC = "$vbucket.HLC";
        public const string FIELD_RECORDS = "records";
        public const string FIELD_CLIENTS = "clients";
        public const string FIELD_CLIENTS_FULL = FIELD_RECORDS + "." + FIELD_CLIENTS;
        public const string FIELD_OVERRIDE = "override";

        [JsonProperty(FIELD_CLIENTS)]
        public Dictionary<string, ClientRecordEntry> Clients { get; init; } = new Dictionary<string, ClientRecordEntry>();

        [JsonProperty(FIELD_OVERRIDE)]
        public ClientRecordsOverride? Override { get; init; } = null;
    }
}
