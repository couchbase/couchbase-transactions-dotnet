using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using DnsClient.Internal;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions
{
    public class AttemptContext
    {
        public ILogger<AttemptContext> Logger { get; }

        public Task<ITransactionGetResult?> GetOptional(ICouchbaseCollection collection, string id) => throw new NotImplementedException();
        public Task<ITransactionGetResult> Get(ICouchbaseCollection collection, string id) => throw new NotImplementedException();
        public Task<ITransactionGetResult> Replace(ITransactionGetResult doc, object content) => throw new NotImplementedException();
        public Task<ITransactionGetResult> Insert(ICouchbaseCollection collection, string id, object content) => throw new NotImplementedException();
        public Task Remove(ITransactionGetResult doc) => throw new NotImplementedException();
        public Task Commit() => throw new NotImplementedException();
        public Task Rollback() => throw new NotImplementedException();
        public Task Defer() => throw new NotImplementedException();
    }
}
