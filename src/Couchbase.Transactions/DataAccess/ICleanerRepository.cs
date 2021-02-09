using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Cleanup.LostTransactions;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;

namespace Couchbase.Transactions.DataAccess
{
    internal interface ICleanerRepository
    {
        string BucketName => Collection.Scope.Bucket.Name;
        string ScopeName => Collection.Scope.Name;
        string CollectionName => Collection.Name;
        ICouchbaseCollection Collection { get; }
        Task<(ClientRecordsIndex? clientRecord, ParsedHLC parsedHlc, ulong? cas)> GetClientRecord();
        Task CreatePlaceholderClientRecord(ulong? cas = null);
        Task RemoveClient(string clientUuid);
        Task UpdateClientRecord(string clientUuid, TimeSpan cleanupWindow, int numAtrs, IReadOnlyList<string> expiredClientIds);

        Task<(Dictionary<string, AtrEntry> attempts, ParsedHLC parsedHlc)> LookupAttempts(string atrId);
    }
}
