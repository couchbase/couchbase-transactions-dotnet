using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataAccess;
using Couchbase.Transactions.DataModel;
using Moq;

namespace Couchbase.Transactions.Tests.UnitTests.Mocks
{
    internal class MockAtrRepository : IAtrRepository
    {
        public string AtrId { get; } = "MockATR" + Guid.NewGuid();
        public string BucketName => "MockAtrBucket";
        public string ScopeName => "MockAtrScope";
        public string CollectionName => "MockAtrCollection";

        public ICouchbaseCollection Collection { get; }

        private string FullId => Collection.GetKey(AtrId);

        public Dictionary<string, AtrEntry> Atrs = new Dictionary<string, AtrEntry>();

        public MockAtrRepository()
        {
            Collection = new MockCollectionWithNames(CollectionName, ScopeName, BucketName);
        }

        public Task<AtrEntry> FindEntryForTransaction(ICouchbaseCollection atrCollection, string atrId, string attemptId)
        {
            if (Atrs.TryGetValue(atrCollection.GetKey(atrId), out var atr))
            {
                return Task.FromResult(atr);
            }

            throw new PathNotFoundException();
        }

        public Task<ICouchbaseCollection> GetAtrCollection(AtrRef atrRef) => Task.FromResult((ICouchbaseCollection)new MockCollectionWithNames(atrRef.CollectionName, atrRef.ScopeName, atrRef.BucketName));

        public Task<string> LookupAtrState()
        {
            if (Atrs.TryGetValue(FullId, out var atr))
            {
                return Task.FromResult(atr.State.ToString());
            }

            throw new PathNotFoundException();
        }

        public Task MutateAtrAborted(IEnumerable<StagedMutation> stagedMutations) => UpdateStateOrThrow(Support.AttemptStates.ABORTED);

        public Task MutateAtrCommit(IEnumerable<StagedMutation> stagedMutations) => UpdateStateOrThrow(Support.AttemptStates.COMMITTED);

        public Task MutateAtrComplete() => UpdateStateOrThrow(Support.AttemptStates.COMPLETED);

        public Task MutateAtrPending(ulong exp)
        {
            var atrEntry = new AtrEntry()
            {
                State = Support.AttemptStates.PENDING,
            };

            Atrs[FullId] = atrEntry;
            return Task.CompletedTask;
        }

        public Task MutateAtrRolledBack() => UpdateStateOrThrow(Support.AttemptStates.ROLLED_BACK);

        private Task UpdateStateOrThrow(Support.AttemptStates state)
        {
            if (Atrs.TryGetValue(FullId, out var atr))
            {
                atr.State = state;
                return Task.CompletedTask;
            }

            throw new PathNotFoundException();
        }
    }
}
