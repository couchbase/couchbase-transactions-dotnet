using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Log;
using Newtonsoft.Json;

namespace Couchbase.Transactions.Components
{
    internal class DocRecord
    {
        [JsonProperty("bkt")]
        public string BucketName { get; }

        [JsonProperty("scp")]
        public string ScopeName { get; }

        [JsonProperty("col")]
        public string CollectionName { get; }

        [JsonProperty("id")]
        public string Id { get; }

        [JsonConstructor]
        public DocRecord(string bkt, string scp, string col, string id)
        {
            BucketName = bkt ?? throw new ArgumentNullException(nameof(bkt));
            ScopeName = scp ?? throw new ArgumentNullException(nameof(scp));
            CollectionName = col ?? throw new ArgumentNullException(nameof(col));
            Id = id ?? throw new ArgumentNullException(nameof(id));
        }

        public async Task<ICouchbaseCollection> GetCollection(ICluster cluster)
        {
            var bucket = await cluster.BucketAsync(BucketName).CAF();
            var scope = bucket.Scope(ScopeName);
            return scope.Collection(CollectionName);
        }
    }
}
