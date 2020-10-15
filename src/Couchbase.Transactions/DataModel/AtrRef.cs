using System;
using System.Collections.Generic;
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

        public async Task<ICouchbaseCollection?> GetAtrCollection(ICouchbaseCollection anyCollection)
        {
            if (BucketName == null || CollectionName == null)
            {
                return null;
            }

            if (anyCollection.Scope.Name == ScopeName
                && anyCollection.Scope.Bucket.Name == BucketName
                && anyCollection.Name == CollectionName)
            {
                return anyCollection;
            }

            var bkt = await anyCollection.Scope.Bucket.Cluster.BucketAsync(BucketName).CAF();
            var scp = ScopeName != null ? bkt.Scope(ScopeName) : bkt.DefaultScope();

            return scp.Collection(CollectionName);
        }

        public override string ToString() => $"{BucketName ?? "-"}/{ScopeName ?? "-"}/{CollectionName ?? "-"}/{Id ?? "-"}";
    }
}
