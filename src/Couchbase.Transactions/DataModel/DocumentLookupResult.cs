using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Internal;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.DataModel
{
    public class DocumentLookupResult
    {
        internal DocumentLookupResult(
            string id,
            IContentAsWrapper? unstagedContent,
            IContentAsWrapper? stagedContent,
            ILookupInResult lookupInResult,
            DocumentMetadata documentMetadata,
            ICouchbaseCollection documentCollection)
        {
            Id = id;
            LookupInResult = lookupInResult;
            StagedContent = stagedContent;
            UnstagedContent = unstagedContent;
            DocumentMetadata = documentMetadata;
            DocumentCollection = documentCollection;
        }

        private ILookupInResult LookupInResult { get; }

        public string Id { get; }

        public TransactionXattrs? TransactionXattrs { get; set; } = null;

        public DocumentMetadata DocumentMetadata { get; }

        public bool IsDeleted => LookupInResult.IsDeleted;

        public ulong Cas => LookupInResult.Cas;

        internal IContentAsWrapper? UnstagedContent { get; }

        internal IContentAsWrapper? StagedContent { get; }

        internal ICouchbaseCollection DocumentCollection { get; }

        public TransactionGetResult GetPreTransactionResult()
        {
            return TransactionGetResult.FromNonTransactionDoc(
                collection: DocumentCollection,
                id: Id,
                content: UnstagedContent ?? throw new ArgumentNullException(nameof(UnstagedContent)),
                cas: Cas,
                documentMetadata: DocumentMetadata);
        }

        public TransactionGetResult GetPostTransactionResult(TransactionJsonDocumentStatus txnJsonStatus)
        {
            return TransactionGetResult.FromStaged(
                DocumentCollection,
                Id,
                StagedContent ?? throw new ArgumentNullException(nameof(StagedContent)),
                Cas,
                DocumentMetadata,
                txnJsonStatus,
                TransactionXattrs
            );
        }
    }
}
