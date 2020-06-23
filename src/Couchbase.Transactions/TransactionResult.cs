using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core;
using DnsClient.Internal;
using Microsoft.Extensions.Logging;

namespace Couchbase.Transactions
{
    public class TransactionResult
    {
        public ILogger<Transactions> Logger { get; internal set; }
        public string TransactionId { get; internal set; }
        public IEnumerable<TransactionAttempt> Attempts { get; internal set; }
        public MutationToken MutationToken { get; internal set; }
    }
}
