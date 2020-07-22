using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Tests.UnitTests.Mocks;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Couchbase.Transactions.Tests.UnitTests
{
    public class TransactionsTests
    {
        private readonly ITestOutputHelper _outputHelper;

        public TransactionsTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
        }

        ////[Fact]
        ////public async Task Run_Basic_Get()
        ////{
        ////    var mockDocs = new List<ObjectGetResult>()
        ////    {
        ////        new ObjectGetResult("found-id", new object())
        ////    };

        ////    using var cluster = CreateTestCluster(mockDocs);
        ////    using var transactions = Transactions.Create(cluster);
        ////    var reachedEnd = false;
        ////    var transactionResult = await transactions.Run(async ctx =>
        ////    {
        ////        var bucket = await cluster.BucketAsync("test-bucket").ConfigureAwait(false);
        ////        var collection = bucket.DefaultCollection();
        ////        var notFoundDoc = await ctx.GetOptional(collection, "notFound-id").ConfigureAwait(false);
        ////        Assert.Null(notFoundDoc);
        ////        reachedEnd = true;
        ////    });

        ////    Assert.True(reachedEnd);
        ////    Assert.NotNull(transactionResult);
        ////    Assert.NotEmpty(transactionResult.Attempts);
        ////}

        [Fact]
        public async Task Canonical_Example_Compiles()
        {
            try
            {
                await CanonicalExample();
            }
            catch (Exception e)
            {
                _outputHelper.WriteLine($"{nameof(Canonical_Example_Compiles)}: Unhandled Exception: {e.ToString()}");
            }
        }

        [Fact]
        public async Task Canonical_Example_Runs_To_End_Without_Throwing()
        {
            var ranToEnd = await CanonicalExample();
            Assert.True(ranToEnd);
        }

        private async Task<bool> CanonicalExample()
        {
            using var cluster = CreateTestCluster(Enumerable.Empty<TransactionGetResult>());

            var configBuilder = TransactionConfigBuilder.Create()
                .DurabilityLevel(DurabilityLevel.Majority);

            if (Debugger.IsAttached)
            {
                configBuilder.ExpirationTime(TimeSpan.FromMinutes(10));
            }

            using var transactions = Transactions.Create(cluster, configBuilder.Build());

            bool reachedPostCommit = false;
            TransactionResult tr = null;
            try
            {
                tr = await transactions.Run(async ctx => 
                {
                    // Inserting a doc:
                    var docId = "test-id";
                    var bucket = await cluster.BucketAsync("test-bucket").ConfigureAwait(false);
                    var collection = bucket.DefaultCollection();
                    var insertResult = await ctx.Insert(collection, docId, new JObject()).ConfigureAwait(false);

                    // Getting documents:
                    var docOpt = await ctx.GetOptional(collection, docId).ConfigureAwait(false);
                    var doc = await ctx.Get(collection, docId).ConfigureAwait(false);
                    
                    // Replacing a document:
                    var anotherDoc = await ctx.Get(collection, "anotherDoc").ConfigureAwait(false);
                    var content = anotherDoc.ContentAs<JObject>();
                    content["transactions"] = "are awesome";
                    await ctx.Replace(anotherDoc, content);

                    // Removing a document:
                    var yetAnotherDoc = await ctx.Get(collection, "yetAnotherDoc)").ConfigureAwait(false);
                    await ctx.Remove(yetAnotherDoc).ConfigureAwait(false);

                    await ctx.Commit().ConfigureAwait(false);
                    reachedPostCommit = true;
                });
            }
            catch (TransactionCommitAmbiguousException e)
            {
                // TODO: log individual errors
                _outputHelper.WriteLine(e.ToString());
                throw;
            }
            catch (TransactionFailedException e)
            {
                // TODO: log errors from exception
                _outputHelper.WriteLine(e.ToString());
                throw;
            }

            Assert.NotNull(tr);
            return reachedPostCommit;
        }

        private ICluster CreateTestCluster(IEnumerable<TransactionGetResult> mockDocs)
        {
            var mockBucket = new Mock<IBucket>(MockBehavior.Strict);
            mockBucket.Setup(b => b.DefaultCollection())
                .Returns(new MockCollection(mockDocs));
            var mockCluster = new Mock<ICluster>(MockBehavior.Strict);
            mockCluster.Setup(c => c.BucketAsync(It.IsAny<string>()))
                .ReturnsAsync(mockBucket.Object);
            mockCluster.Setup(c => c.Dispose());
            mockCluster.Setup(c => c.DisposeAsync());
            mockCluster.SetupGet(c => c.ClusterServices).Returns(new MockClusterServices());
            return mockCluster.Object;
        }
    }
}
