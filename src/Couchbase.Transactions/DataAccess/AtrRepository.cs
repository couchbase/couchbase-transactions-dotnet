using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.DataAccess
{
    internal class AtrRepository : IAtrRepository
    {
        private readonly string _attemptId;
        private readonly TransactionContext _overallContext;
        private readonly string _prefixedAtrFieldDocsInserted;
        private readonly string _prefixedAtrFieldDocsRemoved;
        private readonly string _prefixedAtrFieldDocsReplaced;
        private readonly string _prefixedAtrFieldExpiresAfterMsecs;
        private readonly string _prefixedAtrFieldsPendingSentinel;
        private readonly string _prefixedAtrFieldStartCommit;
        private readonly string _prefixedAtrFieldStartTimestamp;
        private readonly string _prefixedAtrFieldStatus;
        private readonly string _prefixedAtrFieldTimestampComplete;
        private readonly string _prefixedAtrFieldTimestampRollbackComplete;
        private readonly string _prefixedAtrFieldTimestampRollbackStart;
        private readonly string _prefixedAtrFieldTransactionId;

        public string AtrId { get; }

        public ICouchbaseCollection Collection { get; }

        public AtrRepository(string attemptId, TransactionContext overallContext, ICouchbaseCollection atrCollection, string atrId)
        {
            AtrId = atrId;
            var prefix = $"{TransactionFields.AtrFieldAttempts}.{attemptId}";
            _attemptId = attemptId;
            _overallContext = overallContext;
            Collection = atrCollection;
            _prefixedAtrFieldDocsInserted = $"{prefix}.{TransactionFields.AtrFieldDocsInserted}";
            _prefixedAtrFieldDocsRemoved = $"{prefix}.{TransactionFields.AtrFieldDocsRemoved}";
            _prefixedAtrFieldDocsReplaced = $"{prefix}.{TransactionFields.AtrFieldDocsReplaced}";
            _prefixedAtrFieldExpiresAfterMsecs = $"{prefix}.{TransactionFields.AtrFieldExpiresAfterMsecs}";
            _prefixedAtrFieldsPendingSentinel = $"{prefix}.{TransactionFields.AtrFieldPendingSentinel}";
            _prefixedAtrFieldStartCommit = $"{prefix}.{TransactionFields.AtrFieldStartCommit}";
            _prefixedAtrFieldStartTimestamp = $"{prefix}.{TransactionFields.AtrFieldStartTimestamp}";
            _prefixedAtrFieldStatus = $"{prefix}.{TransactionFields.AtrFieldStatus}";
            _prefixedAtrFieldTimestampComplete = $"{prefix}.{TransactionFields.AtrFieldTimestampComplete}";
            _prefixedAtrFieldTimestampRollbackComplete = $"{prefix}.{TransactionFields.AtrFieldTimestampRollbackComplete}";
            _prefixedAtrFieldTimestampRollbackStart = $"{prefix}.{TransactionFields.AtrFieldTimestampRollbackStart}";
            _prefixedAtrFieldTransactionId = $"{prefix}.{TransactionFields.AtrFieldTransactionId}";
        }

        public Task<AtrEntry?> FindEntryForTransaction(ICouchbaseCollection atrCollection, string atrId, string? attemptId = null)
            => FindEntryForTransaction(atrCollection, atrId, attemptId ?? _attemptId, _overallContext?.Config?.KeyValueTimeout);

        public static async Task<AtrEntry?> FindEntryForTransaction(
            ICouchbaseCollection atrCollection,
            string atrId,
            string attemptId,
            TimeSpan? keyValueTimeout = null
            )
        {
            _ = atrCollection ?? throw new ArgumentNullException(nameof(atrCollection));
            _ = atrId ?? throw new ArgumentNullException(nameof(atrId));

            var lookupInResult = await atrCollection.LookupInAsync(atrId,
                specs => specs.Get(TransactionFields.AtrFieldAttempts, isXattr: true),
                opts => opts.Timeout(keyValueTimeout).AccessDeleted(true)).CAF();

            if (!lookupInResult.Exists(0))
            {
                return null;
            }

            var asJson = lookupInResult.ContentAs<JObject>(0);
            if (asJson.TryGetValue(attemptId, out var entry))
            {
                var atrEntry = AtrEntry.CreateFrom(entry);
                if (atrEntry?.Cas == null && atrEntry?.State == default)
                {
                    throw new InvalidOperationException("ATR could not be parsed.");
                }

                return atrEntry;
            }
            else
            {
                return null;
            }
        }

        public static async Task<ICouchbaseCollection?> GetAtrCollection(AtrRef atrRef, ICouchbaseCollection anyCollection)
        {
            if (atrRef.BucketName == null || atrRef.CollectionName == null)
            {
                return null;
            }

            _ = anyCollection?.Scope?.Bucket?.Name ??
                throw new ArgumentOutOfRangeException(nameof(anyCollection), "Collection was not populated.");

            if (anyCollection.Scope.Name == atrRef.ScopeName
                && anyCollection.Scope.Bucket.Name == atrRef.BucketName
                && anyCollection.Name == atrRef.CollectionName)
            {
                return anyCollection;
            }

            var bkt = await anyCollection.Scope.Bucket.Cluster.BucketAsync(atrRef.BucketName).CAF();
            var scp = atrRef.ScopeName != null ? await bkt.ScopeAsync(atrRef.ScopeName) : await bkt.DefaultScopeAsync();

            return await scp.CollectionAsync(atrRef.CollectionName);
        }

        public Task<ICouchbaseCollection?> GetAtrCollection(AtrRef atrRef) => GetAtrCollection(atrRef, Collection);

        public async Task MutateAtrComplete()
        {
            var specs = new []
            {
                MutateInSpec.Upsert(_prefixedAtrFieldStatus, AttemptStates.COMPLETED.ToString(), isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldTimestampComplete, MutationMacro.Cas)
            };

            _ = await Collection.MutateInAsync(AtrId, specs,
                opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();
        }

        public async Task MutateAtrPending(ulong exp)
        {
            var dbg = await Collection.MutateInAsync(AtrId, specs =>
                    specs.Insert(_prefixedAtrFieldTransactionId,
                            _overallContext.TransactionId,
                            createPath: true, isXattr: true)
                        .Insert(_prefixedAtrFieldStatus,
                            AttemptStates.PENDING.ToString(), createPath: false, isXattr: true)
                        .Insert(_prefixedAtrFieldStartTimestamp, MutationMacro.Cas)
                        .Insert(_prefixedAtrFieldExpiresAfterMsecs, exp,
                            createPath: false, isXattr: true),
                opts => opts.StoreSemantics(StoreSemantics.Upsert)
            ).CAF();
        }

        public async Task MutateAtrCommit(IEnumerable<StagedMutation> stagedMutations)
        {
            (var inserts, var replaces, var removes) = SplitMutationsForStaging(stagedMutations);

            var specs = new []
            {
                MutateInSpec.Upsert(_prefixedAtrFieldStatus,
                    AttemptStates.COMMITTED.ToString(), isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldStartCommit, MutationMacro.Cas, isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsInserted, inserts,
                    isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsReplaced, replaces,
                    isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsRemoved, removes,
                    isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldsPendingSentinel, 0,
                    isXattr: true)
            };

            _ = await Collection.MutateInAsync(AtrId, specs,
                opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();
        }

        public async Task MutateAtrAborted(IEnumerable<StagedMutation> stagedMutations)
        {
            (var inserts, var replaces, var removes) = SplitMutationsForStaging(stagedMutations);

            var specs = new MutateInSpec[]
            {
                MutateInSpec.Upsert(_prefixedAtrFieldStatus,
                    AttemptStates.ABORTED.ToString(), isXattr: true, createPath: true),
                MutateInSpec.Upsert(_prefixedAtrFieldTimestampRollbackStart,
                    MutationMacro.Cas, isXattr: true, createPath: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsInserted, inserts,
                    isXattr: true, createPath: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsReplaced, replaces,
                    isXattr: true, createPath: true),
                MutateInSpec.Upsert(_prefixedAtrFieldDocsRemoved, removes,
                    isXattr: true, createPath: true),
            };

            _ = await Collection.MutateInAsync(AtrId, specs,
                opts => opts.StoreSemantics(StoreSemantics.Replace).AccessDeleted(true).CreateAsDeleted(true)).CAF();
        }

        public async Task MutateAtrRolledBack()
        {
            var specs = new MutateInSpec[]
            {
                MutateInSpec.Upsert(_prefixedAtrFieldStatus,
                    AttemptStates.ROLLED_BACK.ToString(), isXattr: true),
                MutateInSpec.Upsert(_prefixedAtrFieldTimestampRollbackComplete,
                    MutationMacro.Cas),
            };

            _ = await Collection.MutateInAsync(AtrId, specs,
                opts => opts.StoreSemantics(StoreSemantics.Replace).AccessDeleted(true).CreateAsDeleted(true)).CAF();
        }

        public async Task<string> LookupAtrState()
        {
            var lookupInResult = await Collection!.LookupInAsync(AtrId,
                    specs => specs.Get(_prefixedAtrFieldStatus, isXattr: true),
                    opts => opts.AccessDeleted(true))
                .CAF();
            var refreshedStatus = lookupInResult.ContentAs<string>(0);
            return refreshedStatus;
        }

        private (JArray inserts, JArray replaces, JArray removes) SplitMutationsForStaging(IEnumerable<StagedMutation> stagedMutations)
        {
            var mutations = stagedMutations.ToList();
            var stagedInserts = mutations.Where(sm => sm.Type == StagedMutationType.Insert);
            var stagedReplaces = mutations.Where(sm => sm.Type == StagedMutationType.Replace);
            var stagedRemoves = mutations.Where(sm => sm.Type == StagedMutationType.Remove);
            var inserts = new JArray(stagedInserts.Select(sm => sm.ForAtr()));
            var replaces = new JArray(stagedReplaces.Select(sm => sm.ForAtr()));
            var removes = new JArray(stagedRemoves.Select(sm => sm.ForAtr()));
            return (inserts, replaces, removes);
        }
    }
}
