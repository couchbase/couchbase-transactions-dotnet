using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;

namespace Couchbase.Transactions
{
    public interface ITransactionGetResult : IGetResult
    {
        string Id { get; }
    }
}
