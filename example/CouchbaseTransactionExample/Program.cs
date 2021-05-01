using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Extensions.Logging;

namespace CouchbaseTransactionExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var clusterOptions = new ClusterOptions()
                {
                    RedactionLevel = Couchbase.Core.Logging.RedactionLevel.Partial,
                    UserName = "Administrator",
                    Password = "password"
                };

                await using var cluster =
                    await Cluster.ConnectAsync("couchbase://localhost", clusterOptions);
                await using var bucket = await cluster.BucketAsync("default");
                var collection = await bucket.DefaultCollectionAsync();
                var sampleDoc = new ExampleTransactionDocument();
                var insertResult = await collection.InsertAsync(sampleDoc.Id,
                        sampleDoc)
                    .ConfigureAwait(false);
                var getResultRoundTrip = await collection.GetAsync(sampleDoc.Id).ConfigureAwait(false);
                var roundTripSampleDoc = getResultRoundTrip.ContentAs<ExampleTransactionDocument>();

                Serilog.Log.Logger = new Serilog.LoggerConfiguration().WriteTo.Console().CreateLogger();
                var configBuilder = TransactionConfigBuilder.Create()
                    .LoggerFactory(new SerilogLoggerFactory())
                    .DurabilityLevel(DurabilityLevel.None);

                if (Debugger.IsAttached)
                {
                    configBuilder.ExpirationTime(TimeSpan.FromMinutes(10));
                }

                var txn = Transactions.Create(cluster, configBuilder.Build());
                var txnResult = await txn.RunAsync(async ctx =>
                {
                    var getResult = await ctx.GetAsync(collection, sampleDoc.Id).ConfigureAwait(false);
                    var docGet = getResult.ContentAs<JObject>();

                    docGet["revision"] = docGet["revision"].Value<int>() + 1;
                    var replaceResult = await ctx.ReplaceAsync(getResult, docGet).ConfigureAwait(false);

                    await ctx.CommitAsync();
                }).ConfigureAwait(false);

                Console.Out.WriteLine(txnResult.ToString());
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e.ToString());
                throw;
            }
        }

        private class ExampleTransactionDocument
        {
            public string Type => nameof(ExampleTransactionDocument);

            public string Id { get; set; } = Guid.NewGuid().ToString();

            public string Content { get; set; } = string.Empty;

            public int Revision { get; set; } = 0;
        }
    }
}
/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2021 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
