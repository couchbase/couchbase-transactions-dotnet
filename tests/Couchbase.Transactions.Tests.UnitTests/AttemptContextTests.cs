using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Tests.UnitTests.Mocks;
using Xunit;

namespace Couchbase.Transactions.Tests.UnitTests
{
    public class AttemptContextTests
    {
        ////[Fact]
        ////public async Task Get_Found_Vs_NotFound()
        ////{
        ////    var mockDocs = new List<TransactionGetResult>()
        ////    {
        ////        new ObjectGetResult("found-id", new object())
        ////    };

        ////    var cluster = MockCollection.CreateMockCluster(mockDocs);
        ////    var ctx = new AttemptContext(new TransactionContext(Guid.NewGuid().ToString(), DateTimeOffset.UtcNow, TransactionConfigBuilder.Create().Build(), null));
        ////    var bucket = await cluster.BucketAsync("test-bucket").ConfigureAwait(false);
        ////    var collection = bucket.DefaultCollection();
        ////    var notFoundDoc = await ctx.GetOptionalAsync(collection, "notFound-id").ConfigureAwait(false);
        ////    Assert.Null(notFoundDoc);
        ////    var foundDoc = await ctx.GetOptionalAsync(collection, "found-id").ConfigureAwait(false);
        ////    Assert.NotNull(foundDoc);
        ////    await Assert.ThrowsAsync<DocumentNotFoundException>(async () =>
        ////    {
        ////        var getThrows = await ctx.GetAsync(collection, "notFound2-id").ConfigureAwait(false);
        ////    });

        ////}
    }
}
