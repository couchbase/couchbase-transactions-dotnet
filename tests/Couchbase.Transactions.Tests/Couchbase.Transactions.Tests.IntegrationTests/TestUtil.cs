using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Tests.IntegrationTests.Fixtures;

namespace Couchbase.Transactions.Tests.IntegrationTests
{
    internal static class TestUtil
    {
        public static async Task<DurabilityLevel> InsertAndDetermineDurability(ICouchbaseCollection defaultCollection, string docId,
            object sampleDoc)
        {
            var durability = DurabilityLevel.Majority;

            try
            {
                _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability).Expiry(TimeSpan.FromMinutes(10)));
            }
            catch (DurabilityImpossibleException)
            {
                // when running on single-node cluster, such as localhost.
                durability = DurabilityLevel.None;
                _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability).Expiry(TimeSpan.FromMinutes(10)));
            }

            return durability;
        }

        public static async Task<(ICouchbaseCollection collection, string docId, object sampleDoc)> PrepSampleDoc(ClusterFixture fixture, [CallerMemberName]string testName = nameof(PrepSampleDoc))
        {
            var defaultCollection = await fixture.GetDefaultCollection();
            var docId = Guid.NewGuid().ToString();
            var sampleDoc = new { type = nameof(testName), foo = "bar", revision = 100 };
            return (defaultCollection, docId, sampleDoc);
        }

        public static Transactions CreateTransaction(ICluster cluster, DurabilityLevel durability)
        {
            var configBuilder = TransactionConfigBuilder.Create();
            configBuilder.DurabilityLevel(durability);
            if (Debugger.IsAttached)
            {
                // don't expire when watching the debugger.
                configBuilder.ExpirationTime(TimeSpan.FromMinutes(1000));
            }

            var txn = Transactions.Create(cluster, configBuilder.Build());
            return txn;
        }
    }
}
