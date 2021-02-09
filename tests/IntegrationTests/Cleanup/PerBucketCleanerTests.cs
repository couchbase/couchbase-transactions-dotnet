using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Transactions.Cleanup;
using Couchbase.Transactions.Cleanup.LostTransactions;
using Couchbase.Transactions.DataAccess;
using Couchbase.Transactions.Tests.IntegrationTests.Fixtures;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Couchbase.Transactions.Tests.IntegrationTests.Cleanup
{
    public class PerBucketCleanerTests : IClassFixture<ClusterFixture>
    {
        private readonly ClusterFixture _fixture;
        private readonly ITestOutputHelper _outputHelper;

        public PerBucketCleanerTests(ClusterFixture fixture, ITestOutputHelper outputHelper)
        {
            _fixture = fixture;
            ClusterFixture.LogLevel = LogLevel.Debug;
            _outputHelper = outputHelper;
        }

        [Fact]
        public async Task DefaultBucket_BasicBackgroundRun()
        {
            var loggerFactory = new ClusterFixture.TestOutputLoggerFactory(_outputHelper);
            var clientUuid = Guid.NewGuid().ToString();
            var cleaner = new Cleaner(_fixture.Cluster, null);
            var collection = await _fixture.OpenDefaultCollection(_outputHelper);
            var repo = new CleanerRepository(collection, null);
            PerBucketCleaner perBucketCleaner = null;
            try
            {
                perBucketCleaner = new PerBucketCleaner(clientUuid, cleaner, repo, TimeSpan.FromSeconds(0.1), loggerFactory);
                await Task.Delay(500);
                Assert.True(perBucketCleaner.Running);
                Assert.NotEqual(0, perBucketCleaner.RunCount);
            }
            finally
            {
                await perBucketCleaner.DisposeAsync();
            }

            var recordFetch = await repo.GetClientRecord();
            Assert.DoesNotContain(recordFetch.clientRecord.Clients, kvp => kvp.Key == clientUuid);

            Assert.False(perBucketCleaner.Running);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public async Task DefaultBucket_MultipleClient(int clientCount)
        {
            var loggerFactory = new ClusterFixture.TestOutputLoggerFactory(_outputHelper);
            var clients = new List<PerBucketCleaner>(clientCount);
            try
            {
                for (int i = 0; i < clientCount; i++)
                {
                    var clientUuid = Guid.NewGuid().ToString();
                    var cleaner = new Cleaner(_fixture.Cluster, null);
                    var collection = await _fixture.OpenDefaultCollection(_outputHelper);
                    var repo = new CleanerRepository(collection, null);
                    var perBucketCleaner = new PerBucketCleaner(clientUuid, cleaner, repo, TimeSpan.FromSeconds(0.1), loggerFactory, startDisabled: true);
                    clients.Add(perBucketCleaner);
                    var details = await perBucketCleaner.ProcessClient(cleanupAtrs: false);
                    Assert.Equal(i+1, details.NumActiveClients);
                }
            }
            finally
            {
                foreach (var client in clients)
                {
                    await client.DisposeAsync();
                }
            }
        }
    }
}
