using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;

namespace Couchbase.Transactions.Tests.IntegrationTests.Fixtures
{
    public class TestSettings
    {
        public string ConnectionString { get; set; }
        public string BucketName { get; set; }

        public bool CleanupTestBucket { get; set; } = true;
    }
}
