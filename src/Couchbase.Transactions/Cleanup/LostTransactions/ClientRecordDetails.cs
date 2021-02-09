using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.Transactions.DataModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Cleanup.LostTransactions
{
    internal class ClientRecordDetails
    {
        public const long NanosecondsPerMillisecond = 1_000_000;
        public IReadOnlyList<string> SortedActiveClientIds { get; }
        public IReadOnlyList<string> ExpiredClientIds { get; }
        public IReadOnlyList<string> ActiveClientIds { get; }

        [JsonIgnore]
        public IReadOnlyList<string> AtrsHandledByThisClient { get; }
        public TimeSpan CheckAtrTimeWindow { get; }

        public int NumActiveClients => ActiveClientIds.Count;
        public int NumExpiredClients => ExpiredClientIds.Count;
        public int NumExistingClients => NumActiveClients + NumExpiredClients;

        public int IndexOfThisClient { get; } = -1;
        public bool OverrideEnabled { get; } = false;
        public DateTimeOffset? OverrideExpires { get; }
        public bool OverrideActive { get; }

        public long CasNowNanos { get; }

        public ClientRecordDetails(ClientRecordsIndex clientRecord, ParsedHLC parsedHlc, string clientUuid, TimeSpan cleanupWindow)
        {
            _ = clientRecord ?? throw new ArgumentNullException(nameof(clientRecord));
            _ = parsedHlc ?? throw new ArgumentNullException(nameof(parsedHlc));
            CasNowNanos = parsedHlc.NowTime.ToUnixTimeMilliseconds() * NanosecondsPerMillisecond;
            OverrideEnabled = clientRecord.Override?.Enabled == true;
            OverrideExpires = clientRecord.Override?.Expires;
            OverrideActive = OverrideEnabled && parsedHlc.NowTime < OverrideExpires;
            var clientCount = clientRecord.Clients?.Count ?? 0;
            var expiredClientIds = new List<string>(clientCount);
            var activeClientIds = new List<string>(clientCount);
            bool thisClientAlreadyExists = false;
            if (clientCount > 0)
            {
                foreach (var kvp in clientRecord.Clients!)
                {
                    var uuid = kvp.Key;
                    var client = kvp.Value;

                    // (Note, do not include this client as expired, as it is about to add itself)
                    if (uuid == clientUuid)
                    {
                        activeClientIds.Add(uuid);
                        thisClientAlreadyExists = true;
                    }
                    else if (client.ParsedMutationCas == null || client.Expires < parsedHlc.NowTime)
                    {
                        expiredClientIds.Add(uuid);
                    }
                    else
                    {
                        activeClientIds.Add(uuid);
                    }
                }
            }

            if (!thisClientAlreadyExists)
            {
                activeClientIds.Add(clientUuid);
            }

            var sortedActiveClientIds = activeClientIds.ToList();
            sortedActiveClientIds.Sort();
            SortedActiveClientIds = sortedActiveClientIds;
            for (int i = 0; i < SortedActiveClientIds.Count; i++)
            {
                if (SortedActiveClientIds[i] == clientUuid)
                {
                    IndexOfThisClient = i;
                    break;
                }
            }

            ExpiredClientIds = expiredClientIds.ToList();
            ActiveClientIds = activeClientIds.ToList();
            AtrsHandledByThisClient = GetAtrsHandledByThisClient().ToList();
            var handledCount = AtrsHandledByThisClient.Count;
            handledCount = handledCount == 0 ? 1 : handledCount;
            CheckAtrTimeWindow = cleanupWindow / handledCount;
        }

        private IEnumerable<string> GetAtrsHandledByThisClient()
        {
            if (IndexOfThisClient < 0)
            {
                yield break;
            }

            foreach (var atr in ActiveTransactionRecords.AtrIds.Nth(IndexOfThisClient, NumActiveClients))
            {
                yield return atr;
            }
        }

        public override string ToString()
        {
            return JObject.FromObject(this).ToString();
        }
    }
}
