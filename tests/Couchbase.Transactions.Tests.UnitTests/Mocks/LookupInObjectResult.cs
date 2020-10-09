using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.KeyValue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Tests.UnitTests.Mocks
{
    internal class LookupInObjectResult : ILookupInResult
    {
        public List<object> SubDocResults { get; }

        public bool IsDeleted { get; set; }

        public LookupInObjectResult(IEnumerable<object> subDocResults)
        {
            SubDocResults = subDocResults?.ToList() ?? new List<object>();
        }

        public ulong Cas { get; set; }
        public bool Exists(int index) => SubDocResults.Count > index && SubDocResults[index] != null;

        public T ContentAs<T>(int index)
        {
            if (!Exists(index))
            {
                return default;
            }

            var doc = SubDocResults[index];
            if (doc is T typeMatched)
            {
                return typeMatched;
            }

            if (doc is JProperty token)
            {
                return token.Value.Value<T>();
            }
            else
            {
                var json = JsonConvert.SerializeObject(doc);
                return JsonConvert.DeserializeObject<T>(json);
            }
        }
    }
}
