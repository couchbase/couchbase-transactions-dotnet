using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Couchbase.Transactions
{
    internal static class ProtocolVersion
    {
        public static readonly decimal SupportedVersion = 2.0m;

        public static IEnumerable<ExtensionName> ExtensionsSupported()
        {
            // these will be the stringified version of the enum generated off of the GRPC proto files.
            // For example, EXT_DEFERRED_COMMIT becomes ExtDeferredCommit
            yield return new ExtensionName("ExtDeferredCommit", "EXT_DEFERRED_COMMIT", "DC");
            yield return new ExtensionName("ExtTransactionId", "EXT_TRANSACTION_ID", "TI");
            yield return new ExtensionName("ExtRemoveCompleted", "EXT_REMOVE_COMPLETED", "RC");
        }

        internal static bool Supported(string shortCode) => SupportedShortCodes.Value.Contains(shortCode);

        private static Lazy<HashSet<string>> SupportedShortCodes => new Lazy<HashSet<string>>(() => ExtensionsSupported().Select(ext => ext.ShortCode).ToHashSet());

        internal record ExtensionName(string PascalCase, string ConstantStyle, string ShortCode);
    }
}
