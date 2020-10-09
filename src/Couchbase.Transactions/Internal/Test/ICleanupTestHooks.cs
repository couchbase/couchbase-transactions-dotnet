using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Couchbase.Transactions.Internal.Test
{
    internal class ICleanupTestHooks
    {
        public Task<int?> BeforeCommitDoc(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeDocGet(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveDocStagedForRemoval(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveDoc(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeAtrGet(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeAtrRemove(string id) => Task.FromResult((int?)1);
        public Task<int?> BeforeRemoveLinks(string id) => Task.FromResult((int?)1);
    }

    internal class DefaultCleanupTestHooks : ICleanupTestHooks
    {
        public static readonly ICleanupTestHooks Instance = new DefaultCleanupTestHooks();
    }
}
