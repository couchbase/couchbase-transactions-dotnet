using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;

namespace ThroughputTester
{
    public class Program
    {
        public static async Task Main()
        {
            // setup cluster and open bucket
            var cluster = new Cluster(new ClientConfiguration
            {
                Servers = new List<Uri> {new Uri("couchbase://10.112.182.101")}
            });
            cluster.Authenticate("Administrator", "password");
            var bucket = cluster.OpenBucket("default");

            // setup transaction client
            var transactionClient = new TransactionClient(
                cluster,
                new TransactionConfigBuilder().Build()
            );

            // execute transactional operations
            var result = await transactionClient.Run(async context =>
            {
                var key = Guid.NewGuid().ToString();
                var content = new {id = key};

                await context.Insert<dynamic>(bucket, key, content).ConfigureAwait(false);

                await context.Commit().ConfigureAwait(false);
            }).ConfigureAwait(false);

            Console.WriteLine($"TransactionID: {result.TransactionId} - Attempts: {result.Attempts.Count} - Duration: {result.Duration.TotalSeconds} seconds");
            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }
    }
}
