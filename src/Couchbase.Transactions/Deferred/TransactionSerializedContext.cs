using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Deferred
{
    public class TransactionSerializedContext
    {
        public string EncodeAsString() => throw new NotImplementedException();
        public byte[] EncodeAsBytes() => throw new NotImplementedException();
    }
}
