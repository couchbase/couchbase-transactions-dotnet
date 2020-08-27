using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Transactions.ActiveTransactionRecords;
using Couchbase.Transactions.Components;
using Newtonsoft.Json;
using Xunit;

namespace Couchbase.Transactions.Tests.UnitTests
{
    public class ActiveTransactionRecordTests
    {
        [Fact]
        public void Get_Atr_Matches_Format()
        {
            var atrId = AtrIds.GetAtrId(nameof(Get_Atr_Matches_Format));
            Assert.StartsWith("_txn:atr-", atrId);
        }

        [Fact]
        public void Get_AtrId_Does_Not_Throw()
        {
            var rand = new Random();
            var nums = Enumerable.Range(0, 1_000_000).Select(i => rand.Next());
            Parallel.ForEach(nums, n =>
            {
                var atrId = AtrIds.GetAtrId(nameof(Get_AtrId_Does_Not_Throw) + "_" + n.ToString());
                Assert.StartsWith("_txn:atr-", atrId);
            });
        }

        [Fact]
        public void Atr_Parse()
        {
            var json = ResourceHelper.ReadResource(@"Data\atr_from_spec.json");
            var atr = JsonConvert.DeserializeObject<ActiveTransactionRecord>(json);
        }

        [Theory]
        [InlineData("0x000058a71dd25c15", 0x155CD21DA7580000 / 1000000L)]
        public void Parse_Mutation_Cas_Field(string cas, long expected)
        {
            var parsed = ActiveTransactionRecord.ParseMutationCasField(cas);
            Assert.NotNull(parsed);
            Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(expected), parsed);
        }
    }
}
