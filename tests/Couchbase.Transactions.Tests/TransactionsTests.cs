using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Moq;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Couchbase.Transactions.Tests
{
    public class TransactionsTests
    {
        private readonly ITestOutputHelper _outputHelper;

        public TransactionsTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
        }

        [Fact]
        public void Canonical_Example_Compiles()
        {
            try
            {
                CanonicalExample();
            }
            catch (Exception e)
            {
                _outputHelper.WriteLine($"{nameof(Canonical_Example_Compiles)}: Unhandled Exception: {e.ToString()}");
            }
        }

        private void CanonicalExample()
        {
            using var cluster = new Mock<ICluster>().Object;

            using var transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
                .DurabilityLevel(DurabilityLevel.Majority)
                .Build());

            try
            {
                transactions.Run(async ctx => 
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
        }
    }
}
