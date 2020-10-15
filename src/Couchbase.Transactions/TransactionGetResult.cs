using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Core.IO.Operations;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Internal;

namespace Couchbase.Transactions
{
    public class TransactionGetResult
    {
        private readonly IContentAsWrapper _content;
        private readonly ITypeTranscoder _transcoder; // TODO: this should no longer be necessary

        public static readonly TransactionGetResult? Empty = null;

        private TransactionGetResult(
            [NotNull] string id,
            [NotNull] IContentAsWrapper content,
            ulong cas,
            [NotNull] ICouchbaseCollection collection,
            TransactionXattrs? transactionXattrs,
            TransactionJsonDocumentStatus status,
            DocumentMetadata? documentMetadata,
            ITypeTranscoder transcoder)
        {
            Id = id;
            FullyQualifiedId = GetFullyQualifiedId(collection, id);
            _content = content;
            Cas = cas;
            Collection = collection;
            TransactionXattrs = transactionXattrs;
            Status = status;
            DocumentMetadata = documentMetadata;
            _transcoder = transcoder;
        }

        internal TransactionJsonDocumentStatus Status { get; }
        internal TransactionXattrs? TransactionXattrs { get; }

        public string Id { get; }
        internal string FullyQualifiedId { get; }
        public ulong Cas { get; internal set; }
        public DocumentMetadata? DocumentMetadata { get; }
        public ICouchbaseCollection Collection { get; }

        public T ContentAs<T>() => _content.ContentAs<T>();

        internal static string GetFullyQualifiedId(ICouchbaseCollection collection, string id) =>
            $"{collection.Scope.Bucket.Name}::{collection.Scope.Name}::{collection.Name}::{id}";

        internal static TransactionGetResult FromInsert(
            ICouchbaseCollection collection,
            string id,
            IContentAsWrapper content,
            string transactionId,
            string attemptId,
            string atrId,
            string atrBucketName,
            string atrScopeName,
            string atrCollectionName,
            IMutateInResult updatedDoc,
            ITypeTranscoder transcoder
            )
        {
            var txn = new TransactionXattrs();
            txn.AtrRef = new AtrRef()
            {
                BucketName =  atrBucketName,
                CollectionName = atrCollectionName,
                ScopeName = atrScopeName,
                Id = atrId
            };

            txn.Id = new CompositeId()
            {
                Transactionid = transactionId,
                AttemptId = attemptId
            };

            return new TransactionGetResult(
                id,
                content,
                updatedDoc.Cas,
                collection,
                txn,
                TransactionJsonDocumentStatus.Normal,
                null,
                transcoder
            );
        }

        internal static TransactionGetResult FromOther(
            TransactionGetResult doc,
            IContentAsWrapper content,
            TransactionJsonDocumentStatus status)
        {
            // TODO: replacement for Links

            return new TransactionGetResult(
                doc.Id,
                content,
                doc.Cas,
                doc.Collection,
                doc.TransactionXattrs,
                status,
                doc.DocumentMetadata,
                doc._transcoder
                );
        }

        [Obsolete]
        internal static TransactionGetResult FromLookupIn(
            ICouchbaseCollection collection,
            string id,
            TransactionJsonDocumentStatus status,
            ITypeTranscoder transcoder,
            ulong lookupInCas,
            IContentAsWrapper preTxnContent,
            string? atrId = null,
            string? transactionId = null,
            string? attemptId = null,
            string? stagedContent = null,
            string? atrBucketName = null,
            string? atrLongCollectionName = null,

            // Read from xattrs.txn.restore
            string? casPreTxn = null,
            string? revidPreTxn = null,
            ulong? exptimePreTxn = null,

            // Read from $document
            string? casFromDocument = null,
            string? revidFromDocument = null,
            ulong? exptimeFromDocument = null,

            string? op = null)
        {
            // TODO: check stagedContent for "<<REMOVE>>" sentinel value
            string? atrCollectionName = null;
            string? atrScopeName = null;
            if (atrLongCollectionName != null)
            {
                var pieces = atrLongCollectionName.Split('.');
                (atrScopeName, atrCollectionName) = (pieces[0], pieces[1]);
            }

            var dm = new DocumentMetadata()
            {
                // TODO: When data access is refactored, make sure Crc32c is included here.
                Cas = casFromDocument, RevId = revidFromDocument, ExpTime = exptimeFromDocument,
            };

            return new TransactionGetResult(
                id,
                preTxnContent,
                lookupInCas,
                collection,
                null, // FIXME
                status,
                dm,
                transcoder);
        }

        ////public static TransactionGetResult FromNonTransactionDoc(ICouchbaseCollection docCollection, DocumentWithTransactionMetadata docWithMeta, ITypeTranscoder transcoder)
        ////{
        ////    return new TransactionGetResult(
        ////        id: docWithMeta.Id,
        ////        content: docWithMeta.PreTransactionContent ?? Array.Empty<byte>(),
        ////        cas: docWithMeta.LookupInResult.Cas,
        ////        collection: docCollection,
        ////        transactionXattrs: null,
        ////        status: TransactionJsonDocumentStatus.Normal,
        ////        documentMetadata: docWithMeta.DocumentMetadata,
        ////        transcoder: transcoder
        ////    );
        ////}

        internal static TransactionGetResult FromNonTransactionDoc(ICouchbaseCollection collection, string id, IContentAsWrapper content, ulong cas, DocumentMetadata documentMetadata, ITypeTranscoder transcoder)
        {
            return new TransactionGetResult(
                id: id,
                content: content,
                cas: cas,
                collection: collection,
                transactionXattrs: null,
                status: TransactionJsonDocumentStatus.Normal,
                documentMetadata: documentMetadata,
                transcoder: transcoder
            );
        }

        internal static TransactionGetResult FromStaged(ICouchbaseCollection collection, string id, IContentAsWrapper stagedContent, ulong cas, DocumentMetadata documentMetadata, TransactionJsonDocumentStatus status, TransactionXattrs? txn, ITypeTranscoder transcoder)
        {
            return new TransactionGetResult(
                id,
                stagedContent,
                cas,
                collection,
                txn,
                status,
                documentMetadata,
                transcoder
                );
        }
    }
}
