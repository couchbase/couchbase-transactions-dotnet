using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions
{
    internal static class ProtocolVersion
    {
        public static readonly string SupportedVersion = "2.0";
        public static IEnumerable<string> ExtensionsSupported()
        {
            // these will be the stringified version of the enum generated off of the GRPC proto files.
            // For example, EXT_DEFERRED_COMMIT becomes ExtDeferredCommit
            yield return "ExtDeferredCommit";
            yield return "ExtTransactionId";
            yield return "ExtRemoveCompleted";
        }
    }
}
