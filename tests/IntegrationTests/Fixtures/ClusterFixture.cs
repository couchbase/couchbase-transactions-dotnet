using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using BucketNotFoundException = Couchbase.Core.Exceptions.BucketNotFoundException;

namespace Couchbase.Transactions.Tests.IntegrationTests.Fixtures
{
    public class ClusterFixture : IAsyncLifetime
    {
        public static readonly string BucketName = "TxnIntegrationTestBucket";
        internal static StringBuilder Logs = new StringBuilder();
        private readonly TestSettings _settings;
        private bool _bucketOpened;

        public static LogLevel LogLevel { get; set; } = LogLevel.Information;

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

        public async Task<ICluster> OpenClusterAsync(ITestOutputHelper outputHelper)
        {
            var opts = GetClusterOptions().WithLogging(new TestOutputLoggerFactory(outputHelper));
            var cluster = await Couchbase.Cluster.ConnectAsync(
                    _settings.ConnectionString,
                    opts)
                .ConfigureAwait(false);

            return cluster;
        }

        public async Task<ICouchbaseCollection> OpenDefaultCollection(ITestOutputHelper outputHelper)
        {
            var cluster = await OpenClusterAsync(outputHelper);
            var bucket = await cluster.BucketAsync(BucketName);
            return bucket.DefaultCollection();
        }

        public async Task InitializeAsync()
        {
            var opts = GetClusterOptions();
            Cluster = await Couchbase.Cluster.ConnectAsync(
                    _settings.ConnectionString,
                    opts)
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

        internal class TestOutputLoggerFactory : ILoggerFactory
        {
            private readonly ITestOutputHelper _outputHelper;

            public TestOutputLoggerFactory(ITestOutputHelper outputHelper)
            {
                _outputHelper = outputHelper;
            }

            public void AddProvider(ILoggerProvider provider)
            {
            }

            public ILogger CreateLogger(string categoryName) => new TestOutputLogger(_outputHelper, categoryName);

            public void Dispose()
            {
            }
        }

        private class TestOutputLogger : ILogger
        {
            private readonly ITestOutputHelper _outputHelper;
            private readonly string _categoryName;

            public TestOutputLogger(ITestOutputHelper outputHelper, string categoryName)
            {
                _outputHelper = outputHelper;
                _categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state) => new Moq.Mock<IDisposable>().Object;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                _outputHelper.WriteLine($"{logLevel}: {_categoryName} [{eventId}] {formatter(state, exception)}");
            }
        }
    }
}
