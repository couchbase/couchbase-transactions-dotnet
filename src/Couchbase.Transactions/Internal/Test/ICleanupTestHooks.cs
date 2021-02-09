using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Transactions.Cleanup.LostTransactions;

namespace Couchbase.Transactions.Internal.Test
{
    public interface ICleanupTestHooks
    {
        public Task<int?> BeforeCommitDoc(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeDocGet(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveDocStagedForRemoval(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveDoc(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeAtrGet(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeAtrRemove(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveLinks(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeGetRecord(string clientUuid) => Task.FromResult<int?>(1);
        public Task<int?> BeforeUpdateRecord(string clientUuid) => Task.FromResult<int?>(1);
        public Task<int?> BeforeCreateRecord(string clientUuid) => Task.FromResult<int?>(1);
        public Task<int?> BeforeRemoveClient(string clientUuid) => Task.FromResult<int?>(1);
    }

    internal class DefaultCleanupTestHooks : ICleanupTestHooks
    {
        public static readonly ICleanupTestHooks Instance = new DefaultCleanupTestHooks();
    }
}
