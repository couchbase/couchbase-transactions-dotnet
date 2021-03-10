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

        public static readonly TransactionGetResult? Empty = null;

        private TransactionGetResult(
            string id,
            IContentAsWrapper? content,
            ulong cas,
            ICouchbaseCollection collection,
            TransactionXattrs? transactionXattrs,
            TransactionJsonDocumentStatus status,
            DocumentMetadata? documentMetadata,
            bool isTombstone)
        {
            Id = id;
            FullyQualifiedId = GetFullyQualifiedId(collection, id);
            _content = content ?? new JObjectContentWrapper(new { });
            Cas = cas;
            Collection = collection;
            TransactionXattrs = transactionXattrs;
            Status = status;
            DocumentMetadata = documentMetadata;
            IsDeleted = isTombstone;
        }

        internal bool IsDeleted { get; }

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
            ulong updatedCas,
            bool isDeleted
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
                updatedCas,
                collection,
                txn,
                TransactionJsonDocumentStatus.Normal,
                null,
                isDeleted
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
                doc.IsDeleted
                );
        }

        internal static TransactionGetResult FromNonTransactionDoc(ICouchbaseCollection collection, string id, IContentAsWrapper content, ulong cas, DocumentMetadata documentMetadata, bool isDeleted, TransactionXattrs? transactionXattrs, TransactionJsonDocumentStatus docStatus)
        {
            return new TransactionGetResult(
                id: id,
                content: content,
                cas: cas,
                collection: collection,
                transactionXattrs: transactionXattrs,
                status: docStatus,
                documentMetadata: documentMetadata,
                isTombstone: isDeleted
            );
        }

        internal static TransactionGetResult FromStaged(ICouchbaseCollection collection, string id, IContentAsWrapper? stagedContent, ulong cas, DocumentMetadata documentMetadata, TransactionJsonDocumentStatus status, TransactionXattrs? txn, bool isTombstone)
        {
            return new TransactionGetResult(
                id,
                stagedContent,
                cas,
                collection,
                txn,
                status,
                documentMetadata,
                isTombstone
                );
        }
    }
}
