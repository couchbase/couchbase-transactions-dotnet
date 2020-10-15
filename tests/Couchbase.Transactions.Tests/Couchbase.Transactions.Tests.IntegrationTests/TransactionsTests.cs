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
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Error.Internal;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Support;
using Couchbase.Transactions.Tests.IntegrationTests.Errors;
using Couchbase.Transactions.Tests.IntegrationTests.Fixtures;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;
using RemoveOptions = Couchbase.KeyValue.RemoveOptions;

namespace Couchbase.Transactions.Tests.IntegrationTests
{
    /// <summary>
    /// Tests written independently of the java or fit performer test suites.
    /// </summary>
    public class TransactionsTests : IClassFixture<ClusterFixture>
    {
        private readonly ClusterFixture _fixture;
        private readonly ITestOutputHelper _outputHelper;

        public TransactionsTests(ClusterFixture fixture, ITestOutputHelper outputHelper)
        {
            _fixture = fixture;
            ClusterFixture.LogLevel = LogLevel.Debug;
            _outputHelper = outputHelper;
        }

        [Fact]
        public async Task Basic_Insert_Should_Succeed()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(Basic_Insert_Should_Succeed), foo = "bar", revision = 100 };
            var docId = nameof(Basic_Insert_Should_Succeed) + Guid.NewGuid().ToString();
            try
            {
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId + "_testDurability", sampleDoc);

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.RunAsync(async ctx =>
                {
                    var insertResult = await ctx.InsertAsync(defaultCollection, docId, sampleDoc).ConfigureAwait(false);
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    Assert.NotNull(getResult);
                    var asJobj = getResult!.ContentAs<JObject>();
                    Assert.Equal("bar", asJobj["foo"].Value<string>());
                });

                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.NotEmpty(result.Attempts);
                Assert.Contains(result.Attempts, ta => ta.FinalState == AttemptStates.COMMITTED
                                                       || ta.FinalState == AttemptStates.COMPLETED);

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
                finally
                {
                    _outputHelper.WriteLine("\n\n==== CB Logs ====");
                    _fixture.DumpLogs(_outputHelper);
                }
            }
        }

        [Fact]
        public async Task Basic_Replace_Should_Succeed()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new {type = nameof(Basic_Replace_Should_Succeed), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId, sampleDoc);

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.RunAsync(async ctx =>
                {
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet);
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                Assert.Contains(result.Attempts, ta => ta.FinalState == AttemptStates.COMMITTED
                || ta.FinalState == AttemptStates.COMPLETED);

                var postTxnGetResult = await defaultCollection.GetAsync(docId);
                var postTxnDoc = postTxnGetResult.ContentAs<dynamic>();
                Assert.Equal("101", postTxnDoc.revision.ToString());

                await txn.DisposeAsync();
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
                finally
                {
                    _fixture.DumpLogs(_outputHelper);
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
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId, sampleDoc);

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.RunAsync(async ctx =>
                {
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    await ctx.RemoveAsync(getResult);
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
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId, sampleDoc);

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                var result = await txn.RunAsync(async ctx =>
                {
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet);
                    await ctx.RollbackAsync();
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
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId, sampleDoc);

                var txn = Transactions.Create(_fixture.Cluster);
                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);

                int attemptCount = 0;
                var runTask = txn.RunAsync(async ctx =>
                {
                    attemptCount++;
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet);
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
                var result =await txn.RunAsync(async ctx =>
                {
                    attemptCount++;
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet);
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
                finally
                {
                    _fixture.DumpLogs(_outputHelper);
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

        [Fact]
        public async Task Get_Repeated_Failures_Should_Throw_TransactionFailed()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var docId = Guid.NewGuid().ToString();

            var txn = Transactions.Create(_fixture.Cluster);
            int attempts = 0;
            txn.TestHooks = new DelegateTestHooks()
            {
                BeforeDocGetImpl = (ctx, id) => throw new InternalIntegrationTestException()
                    {
                        CausingErrorClass = attempts++ < 5 ? ErrorClass.FailTransient : ErrorClass.FailOther
                    }
            };

            var runTask = txn.RunAsync(async ctx =>
            {
                var getResult = await ctx.GetAsync(defaultCollection, docId);
                var docGet = getResult?.ContentAs<dynamic>();
                Assert.False(true, "Should never have reached here.");
            });

            var transactionFailedException = await Assert.ThrowsAsync<TransactionFailedException>(() => runTask);
            Assert.NotNull(transactionFailedException.Result);
            Assert.NotEmpty(transactionFailedException.Result.Attempts);
            Assert.NotInRange(transactionFailedException.Result.Attempts.Count(), 0, 2);
        }

        [Fact]
        public async Task DocumentLookup_Should_Include_Metadata()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(DocumentLookup_Should_Include_Metadata), foo = "bar", revision = 100 };
            var docId = Guid.NewGuid().ToString();
            try
            {
                var durability = await TestUtil.InsertAndDetermineDurability(defaultCollection, docId, sampleDoc);

                var configBuilder = TransactionConfigBuilder.Create();
                configBuilder.DurabilityLevel(durability);
                if (Debugger.IsAttached)
                {
                    configBuilder.ExpirationTime(TimeSpan.FromMinutes(10));
                }

                var txn = Transactions.Create(_fixture.Cluster, configBuilder);


                txn.TestHooks = new DelegateTestHooks()
                {
                    BeforeDocCommittedImpl = async (ctx, id) =>
                    {
                        var documentLookupResult =
                            await DocumentLookupResult.LookupDocumentAsync(defaultCollection, id, null, true);

                        ////Assert.NotNull(documentLookupResult.DocumentAs<JObject>());
                        ////Assert.NotNull(documentLookupResult?.TransactionXattrs);
                        ////_outputHelper.WriteLine(JObject.FromObject(documentLookupResult!.TransactionXattrs).ToString());

                        return 0;
                    },

                    AfterStagedReplaceCompleteImpl = async (ctx, id) =>
                    {
                        var documentLookupResult =
                            await DocumentLookupResult.LookupDocumentAsync(defaultCollection, id, null, true);

                        ////Assert.NotNull(documentLookupResult.DocumentAs<JObject>());
                        ////Assert.NotNull(documentLookupResult?.TransactionXattrs);
                        ////_outputHelper.WriteLine(JObject.FromObject(documentLookupResult!.TransactionXattrs).ToString());

                        return 0;
                    }
                };

                var result = await txn.RunAsync(async ctx =>
                {
                    var getResult = await ctx.GetAsync(defaultCollection, docId);
                    var docGet = getResult!.ContentAs<dynamic>();

                    docGet.revision = docGet.revision + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet);

                    var documentLookupResult =
                        await DocumentLookupResult.LookupDocumentAsync(defaultCollection, docId, null, true);

                    Assert.NotNull(documentLookupResult?.TransactionXattrs);
                    Assert.NotNull(documentLookupResult.StagedContent?.ContentAs<object>());
                    _outputHelper.WriteLine(JObject.FromObject(documentLookupResult!.TransactionXattrs).ToString());
                });

                Assert.NotEmpty(result.Attempts);
                _outputHelper.WriteLine(string.Join(",", result.Attempts));
                ////await txn.DisposeAsync();
            }
            finally
            {
                _fixture.DumpLogs(_outputHelper);
            }
        }

        [Fact]
        public async Task DocumentLookup_Basic()
        {
            var defaultCollection = await _fixture.GetDefaultCollection();
            var sampleDoc = new { type = nameof(DocumentLookup_Basic), foo = "bar", revision = 100, sub = new { a = 1, b = 2 } };
            var docId = Guid.NewGuid().ToString();
            var insertResult =
                await defaultCollection.InsertAsync(docId, sampleDoc, opts => opts.Durability(DurabilityLevel.None));
            var mutateResult =
                await defaultCollection.MutateInAsync(docId, specs =>
                        specs.Upsert("txn.id.txn", "tid1", createPath: true, isXattr: true)
                            .Upsert("txn.id.atmpt", "atmptid1", createPath: true, isXattr: true),
                    opts => opts.CreateAsDeleted(true)
                        .Cas(insertResult.Cas)
                        .Durability(DurabilityLevel.None)
                        .StoreSemantics(StoreSemantics.Replace));

            var docLookup = await DocumentLookupResult.LookupDocumentAsync(defaultCollection, docId, null, true);
            Assert.NotNull(docLookup.TransactionXattrs);
        }
    }
}
