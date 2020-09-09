using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using Microsoft.Extensions.Configuration;
using Xunit;
using BucketNotFoundException = Couchbase.Core.Exceptions.BucketNotFoundException;

namespace Couchbase.Transactions.Tests.IntegrationTests.Fixtures
{
    public class ClusterFixture : IAsyncLifetime
    {
        public static readonly string BucketName = "TxnIntegrationTestBucket";

        private readonly TestSettings _settings;
        private bool _bucketOpened;

        public ClusterOptions ClusterOptions { get; }

        public ICluster Cluster { get; private set; }

        public ClusterFixture()
        {
            _settings = GetSettings();
            ClusterOptions = GetClusterOptions();
        }

        public async ValueTask<ICluster> GetCluster()
        {
            if (_bucketOpened)
            {
                return Cluster;
            }

            await GetDefaultBucket().ConfigureAwait(false);
            return Cluster;
        }

        public async Task<IBucket> GetDefaultBucket()
        {
            var bucket = await Cluster.BucketAsync(BucketName).ConfigureAwait(false);

            _bucketOpened = true;

            return bucket;
        }

        public async Task<ICouchbaseCollection> GetDefaultCollection()
        {
            var bucket = await GetDefaultBucket().ConfigureAwait(false);
            return bucket.DefaultCollection();
        }

        internal static TestSettings GetSettings()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("config.json")
                .Build()
                .GetSection("testSettings")
                .Get<TestSettings>();
        }

        internal static ClusterOptions GetClusterOptions()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("config.json")
                .Build()
                .GetSection("couchbase")
                .Get<ClusterOptions>();
        }

        public async Task InitializeAsync()
        {
            Cluster = await Couchbase.Cluster.ConnectAsync(
                    _settings.ConnectionString,
                    GetClusterOptions())
                .ConfigureAwait(false);

            var bucketSettings = new BucketSettings() 
                {
                    BucketType = BucketType.Couchbase, 
                    Name = BucketName,
                    RamQuotaMB = 100,
                    NumReplicas = 0
                };

            try
            {
                await Cluster.Buckets.CreateBucketAsync(bucketSettings).ConfigureAwait(false);
            }
            catch (BucketExistsException)
            {
            }
        }

        public async Task DisposeAsync()
        {
            if (Cluster == null)
            {
                return;
            }

            if (_settings.CleanupTestBucket)
            {
                try
                {
                    await Cluster.Buckets.DropBucketAsync(BucketName);
                }
                catch (BucketNotFoundException)
                {
                }
            }

            Cluster.Dispose();
        }
    }
}
