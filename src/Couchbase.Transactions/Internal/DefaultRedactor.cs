using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core.Logging;

namespace Couchbase.Transactions.Internal
{
    internal class DefaultRedactor : IRedactor
    {
        public static readonly IRedactor Instance = new DefaultRedactor();

        public object UserData(object message) => "REDACTED_USER_DATA";

        public object MetaData(object message) => "REDACTED_METADATA";

        public object SystemData(object message) => "REDACTED_SYSTEM_DATA";
    }
}
