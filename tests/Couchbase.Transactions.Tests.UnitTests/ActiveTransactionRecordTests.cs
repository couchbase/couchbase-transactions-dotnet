using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Transactions.ActiveTransactionRecords;
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
    }
}
