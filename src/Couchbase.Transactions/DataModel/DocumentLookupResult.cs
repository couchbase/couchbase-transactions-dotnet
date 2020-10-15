﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
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
        private DocumentLookupResult(
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

        public TransactionGetResult GetPreTransactionResult(ITypeTranscoder transcoder)
        {
            return TransactionGetResult.FromNonTransactionDoc(
                collection: DocumentCollection,
                id: Id,
                content: UnstagedContent ?? throw new ArgumentNullException(nameof(UnstagedContent)),
                cas: Cas,
                documentMetadata: DocumentMetadata,
                transcoder: transcoder);
        }

        public TransactionGetResult GetPostTransactionResult(ITypeTranscoder transcoder, TransactionJsonDocumentStatus txnJsonStatus)
        {
            return TransactionGetResult.FromStaged(
                DocumentCollection,
                Id,
                StagedContent ?? throw new ArgumentNullException(nameof(StagedContent)),
                Cas,
                DocumentMetadata,
                txnJsonStatus,
                TransactionXattrs,
                transcoder
            );
        }

        public static async Task<DocumentLookupResult> LookupDocumentAsync(ICouchbaseCollection collection, string id, TimeSpan? keyValueTimeout, bool fullDocument = true)
        {
            var specs = new List<LookupInSpec>()
            {
                LookupInSpec.Get(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true),
                LookupInSpec.Get("$document", isXattr: true),
                LookupInSpec.Get(TransactionFields.StagedData, isXattr: true)
            };

            int? fullDocIndex = null;
            if (fullDocument)
            {
                specs.Add(LookupInSpec.GetFull());
                fullDocIndex = specs.Count - 1;
            }

            var lookupInResult = await collection.LookupInAsync(id, specs, opts =>
                opts.AccessDeleted(true).Timeout(keyValueTimeout)).CAF();

            var docMeta = lookupInResult.ContentAs<DocumentMetadata>(1);

            IContentAsWrapper? unstagedContent = fullDocIndex.HasValue
                ? new LookupInContentAsWrapper(lookupInResult, fullDocIndex.Value)
                : null;

            var stagedContent = lookupInResult.Exists(2)
                ? new LookupInContentAsWrapper(lookupInResult, 2)
                : null;

            var result = new DocumentLookupResult(id,
                unstagedContent,
                stagedContent,
                lookupInResult,
                docMeta,
                collection);

            if (lookupInResult.Exists(0))
            {
                var jobj = lookupInResult.ContentAs<JObject>(0);
                result.TransactionXattrs = lookupInResult.ContentAs<TransactionXattrs>(0);
            }

            return result;
        }
    }
}