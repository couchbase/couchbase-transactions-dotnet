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
                tr = await transactions.RunAsync(async ctx =>
                {
                    // Inserting a doc:
                    var docId = "test-id";
                    var bucket = await cluster.BucketAsync("test-bucket").ConfigureAwait(false);
                    var collection = bucket.DefaultCollection();
                    var insertResult = await ctx.InsertAsync(collection, docId, new JObject()).ConfigureAwait(false);

                    // Getting documents:
                    var docOpt = await ctx.GetOptional(collection, docId).ConfigureAwait(false);
                    var doc = await ctx.GetAsync(collection, docId).ConfigureAwait(false);

                    // Replacing a document:
                    var anotherDoc = await ctx.GetAsync(collection, "anotherDoc").ConfigureAwait(false);
                    var content = anotherDoc.ContentAs<JObject>();
                    content["transactions"] = "are awesome";
                    await ctx.ReplaceAsync(anotherDoc, content);

                    // Removing a document:
                    var yetAnotherDoc = await ctx.GetAsync(collection, "yetAnotherDoc)").ConfigureAwait(false);
                    await ctx.RemoveAsync(yetAnotherDoc).ConfigureAwait(false);

                    await ctx.CommitAsync().ConfigureAwait(false);
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
            var mockCollection = new MockCollection(mockDocs);
            var mockBucket = new Mock<IBucket>(MockBehavior.Strict);
            mockBucket.SetupGet(b => b.Name).Returns("MockBucket");
            mockBucket.Setup(b => b.DefaultCollection())
                .Returns(mockCollection);
            var mockScope = new Mock<IScope>(MockBehavior.Strict);
            mockScope.SetupGet(s => s.Name).Returns("MockScope");
            mockScope.SetupGet(s => s.Bucket).Returns(mockBucket.Object);
            mockCollection.Scope = mockScope.Object;
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
