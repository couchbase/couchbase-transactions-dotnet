using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.DataAccess
{
    internal interface IAtrRepository
    {
        string AtrId { get; }

        string BucketName => Collection.Scope.Bucket.Name;

        string ScopeName => Collection.Scope.Name;

        string CollectionName => Collection.Name;

        string FullPath => $"{BucketName}.{ScopeName}.{CollectionName}::{AtrId}";

        ICouchbaseCollection Collection { get; }

        Task<ICouchbaseCollection?> GetAtrCollection(AtrRef atrRef);

        Task<string> LookupAtrState();

        Task MutateAtrComplete();

        Task MutateAtrPending(ulong exp);

        Task MutateAtrCommit(IEnumerable<StagedMutation> stagedMutations);

        Task MutateAtrAborted(IEnumerable<StagedMutation> stagedMutations);

        Task MutateAtrRolledBack();

        Task<AtrEntry?> FindEntryForTransaction(ICouchbaseCollection atrCollection, string atrId);

    }
}
