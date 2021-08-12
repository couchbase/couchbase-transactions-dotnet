using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Compatibility;
using Couchbase.Core.IO.Serializers;
using Couchbase.Query;

namespace Couchbase.Transactions
{
    /// <summary>
    /// A limited subset of <see cref="QueryOptions"/> that are usable in Transactions.
    /// </summary>
    public class TransactionQueryOptions
    {
        internal QueryOptions Builder { get; } = new QueryOptions().Metrics(true);

        private TransactionQueryOptions()
        {
        }

        public static TransactionQueryOptions QueryOptions() => new TransactionQueryOptions();

        public TransactionQueryOptions Parameter(string key, object val) => Wrap(() => Builder.Parameter(key, val));
        public TransactionQueryOptions Parameter(object paramValue) => Wrap(() => Builder.Parameter(paramValue));
        public TransactionQueryOptions Parameter(params object[] values) => Wrap(() => Builder.Parameter(values));
        public TransactionQueryOptions ScanConsistency(QueryScanConsistency scanConsistency) => Wrap(() => Builder.ScanConsistency(scanConsistency));
        public TransactionQueryOptions FlexIndex(bool flexIndex) => Wrap(() => Builder.FlexIndex(flexIndex));

        public TransactionQueryOptions Serializer(ITypeSerializer serializer)
        {
            Builder.Serializer = serializer;
            return this;
        }

        public TransactionQueryOptions ClientContextId(string clientContextId) => Wrap(() => Builder.ClientContextId(clientContextId));
        public TransactionQueryOptions ScanWait(TimeSpan scanWait) => Wrap(() => Builder.ScanWait(scanWait));
        public TransactionQueryOptions ScanCap(int capacity) => Wrap(() => Builder.ScanCap(capacity));
        public TransactionQueryOptions PipelineBatch(int batchSize) => Wrap(() => Builder.PipelineBatch(batchSize));
        public TransactionQueryOptions PipelineCap(int capacity) => Wrap(() => Builder.PipelineCap(capacity));
        public TransactionQueryOptions Readonly(bool readOnly) => Wrap(() => Builder.Readonly(readOnly));
        public TransactionQueryOptions AdHoc(bool adhoc) => Wrap(() => Builder.AdHoc(adhoc));
        public TransactionQueryOptions Raw(string key, object val) => Wrap(() => Builder.Raw(key, val));




        public TransactionQueryOptions Timeout(TimeSpan timeout) => Wrap(() => Builder.Timeout(timeout));

        private TransactionQueryOptions Wrap(Func<QueryOptions> wrap)
        {
            wrap();
            return this;
        }
    }
}
