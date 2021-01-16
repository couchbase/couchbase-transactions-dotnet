using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core;
using Couchbase.KeyValue;
using Couchbase.Transactions.DataModel;

namespace Couchbase.Transactions.DataAccess
{
    internal interface IDocumentRepository
    {
        Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedInsert(ICouchbaseCollection collection, string docId, object content, IAtrRepository atr, ulong? cas = null);
        Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedReplace(TransactionGetResult doc, object content, IAtrRepository atr, bool accessDeleted);
        Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedRemove(TransactionGetResult doc, IAtrRepository atr);

        Task<(ulong updatedCas, MutationToken? mutationToken)> UnstageInsertOrReplace(ICouchbaseCollection collection, string docId, ulong cas, object finalDoc, bool insertMode);
        Task UnstageRemove(ICouchbaseCollection collection, string docId);

        Task ClearTransactionMetadata(ICouchbaseCollection collection, string docId, ulong cas, bool isDeleted);

        Task<DocumentLookupResult> LookupDocumentAsync(ICouchbaseCollection collection, string docId, bool fullDocument = true);
    }
}
