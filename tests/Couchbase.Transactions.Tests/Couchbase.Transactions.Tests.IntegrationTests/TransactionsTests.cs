#nullable enable
using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Core.Retry;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Support;
using Couchbase.Transactions.Tests.IntegrationTests.Fixtures;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;
using RemoveOptions = Couchbase.KeyValue.RemoveOptions;

namespace Couchbase.Transactions.Tests.IntegrationTests
{
    public class TransactionsTests : IClassFixture<ClusterFixture>
    {
        private readonly ClusterFixture _fixture;
        private readonly ITestOutputHelper _outputHelper;

        public TransactionsTests(ClusterFixture fixture, ITestOutputHelper outputHelper)
        {
            _fixture = fixture;
            _outputHelper = outputHelper;
        }

        [Fact]
        public async Task Basic_Replace_Should_Succeed()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new {type = nameof(Basic_Replace_Should_Succeed), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = DurabilityLevel.Majority;

                try
                {
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }
                catch (DurabilityImpossibleException)
                {
                    // when running on single-node cluster, such as localhost.
                    durability = DurabilityLevel.None;
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.Run(async ctx =>
                {
                    var getResult = await ctx.Get(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.Replace(getResult, docGet);
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.Contains(result.Attempts, ta => ta.FinalState == AttemptStates.COMMITTED
                || ta.FinalState == AttemptStates.COMPLETED);

                var postTxnGetResult = await defaultCollection.GetAsync(docId);
                var postTxnDoc = postTxnGetResult.ContentAs<dynamic>();
                Assert.Equal("101", postTxnDoc.revision.ToString());
            }
            finally
            {
                try
                {
                    await defaultCollection.RemoveAsync(docId);
                }
                catch (Exception e)
                {
                    _outputHelper.WriteLine($"Error during cleanup: {e.ToString()}");
                    throw;
                }
            }
        }

        [Fact]
        public async Task Basic_Remove_Should_Succeed()
        {
            bool removed = false;
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(Basic_Remove_Should_Succeed), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = DurabilityLevel.Majority;

                try
                {
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }
                catch (DurabilityImpossibleException)
                {
                    // when running on single-node cluster, such as localhost.
                    durability = DurabilityLevel.None;
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.Run(async ctx =>
                {
                    var getResult = await ctx.Get(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    await ctx.Remove(getResult);
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.Contains(result.Attempts, ta => ta.FinalState == AttemptStates.COMMITTED
                                                       || ta.FinalState == AttemptStates.COMPLETED);

                await Assert.ThrowsAsync<DocumentNotFoundException>(() => defaultCollection.GetAsync(docId));
                removed = true;
            }
            finally
            {
                try
                {
                    if (!removed)
                    {
                        await defaultCollection.RemoveAsync(docId);
                    }
                }
                catch (Exception e)
                {
                    _outputHelper.WriteLine($"Error during cleanup: {e.ToString()}");
                    throw;
                }
            }
        }

        [Fact]
        public async Task Basic_Rollback_Should_Result_In_No_Changes()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(Basic_Rollback_Should_Result_In_No_Changes), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = DurabilityLevel.Majority;

                try
                {
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }
                catch (DurabilityImpossibleException)
                {
                    // when running on single-node cluster, such as localhost.
                    durability = DurabilityLevel.None;
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.Run(async ctx =>
                {
                    var getResult = await ctx.Get(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.Replace(getResult, docGet);
                    await ctx.Rollback();
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.Contains(result.Attempts,
                    ta => ta.FinalState == AttemptStates.ROLLED_BACK);

                var postTxnGetResult = await defaultCollection.GetAsync(docId);
                var postTxnDoc = postTxnGetResult.ContentAs<dynamic>();
                Assert.Equal("100", postTxnDoc.revision.ToString());
            }
            finally
            {
                try
                {
                    await defaultCollection.RemoveAsync(docId);
                }
                catch (Exception e)
                {
                    _outputHelper.WriteLine($"Error during cleanup: {e.ToString()}");
                    throw;
                }
            }
        }

        [Fact]
        public async Task Exception_Rollback_Should_Result_In_No_Changes()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(Exception_Rollback_Should_Result_In_No_Changes), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = DurabilityLevel.Majority;

                try
                {
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }
                catch (DurabilityImpossibleException)
                {
                    // when running on single-node cluster, such as localhost.
                    durability = DurabilityLevel.None;
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                int attemptCount = 0;
                var runTask = txn.Run(async ctx =>
                {
                    attemptCount++;
                    var getResult = await ctx.Get(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.Replace(getResult, docGet);
                    throw new InvalidOperationException("Forcing rollback.");
                });

                await Assert.ThrowsAsync<TransactionFailedException>(() => runTask);
                Assert.Equal(1, attemptCount);

                var postTxnGetResult = await defaultCollection.GetAsync(docId);
                var postTxnDoc = postTxnGetResult.ContentAs<dynamic>();
                Assert.Equal("100", postTxnDoc.revision.ToString());
            }
            finally
            {
                try
                {
                    await defaultCollection.RemoveAsync(docId);
                }
                catch (Exception e)
                {
                    _outputHelper.WriteLine($"Error during cleanup: {e.ToString()}");
                    throw;
                }
            }
        }

        [Fact]
        public async Task Retry_On_Certain_Failures()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(Retry_On_Certain_Failures), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = DurabilityLevel.Majority;

                try
                {
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }
                catch (DurabilityImpossibleException)
                {
                    // when running on single-node cluster, such as localhost.
                    durability = DurabilityLevel.None;
                    _ = await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(durability));
                }

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                int attemptCount = 0;
                var result =await txn.Run(async ctx =>
                {
                    attemptCount++;
                    var getResult = await ctx.Get(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.Replace(getResult, docGet);
                    if (attemptCount < 3)
                    {
                        throw new TestRetryException("force retry", new InvalidOperationException());
                    }
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.Contains(result.Attempts, ta => ta.FinalState == AttemptStates.COMMITTED
                                                       || ta.FinalState == AttemptStates.COMPLETED);

                var postTxnGetResult = await defaultCollection.GetAsync(docId);
                var postTxnDoc = postTxnGetResult.ContentAs<dynamic>();
                Assert.Equal("101", postTxnDoc.revision.ToString());
            }
            catch (Exception ex)
            {
                _outputHelper.WriteLine($"Error during main try: {ex.ToString()}");
                throw;
            }
            finally
            {
                try
                {
                    await defaultCollection.RemoveAsync(docId);
                }
                catch (Exception e)
                {
                    _outputHelper.WriteLine($"Error during cleanup: {e.ToString()}");
                    throw;
                }
            }
        }
        private class TestRetryException : Exception, IRetryable
        {
            public TestRetryException(string? message, Exception? inner)
                : base(message, inner)
            {
            }
        }
    }
}
