using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Diagnostics.Tracing;
using Couchbase.KeyValue;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Components
{
    internal class ActiveTransactionRecord
    {
        public const string Remove = "<<REMOVE>>";
        public static readonly byte[] RemovePlaceholderBytes = Encoding.UTF8.GetBytes(Remove);

        [JsonProperty("attempts")]
        public Dictionary<string, AtrEntry> Attempts { get; set; } = new Dictionary<string, AtrEntry>();

        public static async Task<AtrEntry?> FindEntryForTransaction(
            ICouchbaseCollection atrCollection,
            string atrId,
            string attemptId,
            string transactionId,
            TimeSpan timeout,
            TransactionConfig config,
            IInternalSpan? span = null
            )
        {
            _ = atrCollection ?? throw new ArgumentNullException(nameof(atrCollection));
            _ = atrId ?? throw new ArgumentNullException(nameof(atrId));
            _ = attemptId ?? throw new ArgumentNullException(nameof(attemptId));
            _ = transactionId ?? throw new ArgumentNullException(nameof(transactionId));
            _ = config ?? throw new ArgumentNullException(nameof(config));

            var lookupInResult = await atrCollection.LookupInAsync(atrId,
                specs => specs.Get(TransactionFields.AtrFieldAttempts, isXattr: true),
                opts => opts.Timeout(config.KeyValueTimeout).AccessDeleted(true)).CAF();

            return FindEntryForTransaction(atrCollection, atrId, lookupInResult.ContentAs<JObject>(0), attemptId,
                transactionId, lookupInResult.Cas);
        }

        public static AtrEntry? FindEntryForTransaction(
            ICouchbaseCollection atrCollection,
            string atrId,
            JObject json,
            string attemptId,
            string transactionId,
            ulong? cas)
        {
            if (json.TryGetValue(attemptId, out var entry))
            {
                return CreateFrom(atrCollection.Scope.Bucket.Name, atrId, entry, attemptId, transactionId, cas);
            }
            else
            {
                return null;
            }
        }

        public static AtrEntry? CreateFrom(string bucketName, string atrId, JToken entry, string attemptId, string transactionId, ulong? cas)
        {
            _ = entry ?? throw new ArgumentNullException(nameof(entry));
            _ = attemptId ?? throw new ArgumentNullException(nameof(attemptId));
            _ = transactionId ?? throw new ArgumentNullException(nameof(transactionId));

            return entry.ToObject<AtrEntry>();
        }

        internal static DateTimeOffset? ParseMutationCasField(JToken entry, string fieldName)
        {
            var casString = entry.Value<string>(fieldName);
            if (casString == null)
            {
                return null;
            }

            return ParseMutationCasField(casString);
        }

        // ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and,
        // though there is consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are
        // clients (SyncGateway) that consume the current string, so it can't be changed.  Note that only little-endian
        // servers are supported for Couchbase, so the 8 byte long inside the string will always be little-endian ordered.
        //
        // Looks like: "0x000058a71dd25c15"
        // Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch time in millionths of a second)
        internal static DateTimeOffset? ParseMutationCasField(string? casString)
        {
            if (string.IsNullOrWhiteSpace(casString))
            {
                return null;
            }

            int offsetIndex = 2; // for the initial "0x"
            long result = 0;

            for (int octetIndex = 7; octetIndex >= 0; octetIndex -= 1)
            {
                char char1 = casString[offsetIndex + (octetIndex * 2)];
                char char2 = casString[offsetIndex + (octetIndex * 2) + 1];

                long octet1 = 0;
                long octet2 = 0;

                if (char1 >= 'a' && char1 <= 'f')
                    octet1 = char1 - 'a' + 10;
                else if (char1 >= 'A' && char1 <= 'F')
                    octet1 = char1 - 'A' + 10;
                else if (char1 >= '0' && char1 <= '9')
                    octet1 = char1 - '0';
                else
                    throw new InvalidOperationException("Could not parse CAS " + casString);

                if (char2 >= 'a' && char2 <= 'f')
                    octet2 = char2 - 'a' + 10;
                else if (char2 >= 'A' && char2 <= 'F')
                    octet2 = char2 - 'A' + 10;
                else if (char2 >= '0' && char2 <= '9')
                    octet2 = char2 - '0';
                else
                    throw new InvalidOperationException("Could not parse CAS " + casString);

                result |= (octet1 << ((octetIndex * 8) + 4));
                result |= (octet2 << (octetIndex * 8));
            }

            // It's in millionths of a second
            var millis = (long) result / 1000000L;
            return DateTimeOffset.FromUnixTimeMilliseconds(millis);
        }

        private static List<DocRecord> ProcessDocumentIdArray(JToken entry, string fieldName)
        {
            throw new NotImplementedException();
        }
    }
}
