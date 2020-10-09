using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Couchbase.Core.IO.Operations;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;

namespace Couchbase.Transactions
{
    public class TransactionGetResult
    {
        private readonly byte[] _content;
        private readonly ITypeTranscoder _transcoder;

        public static readonly TransactionGetResult? Empty = null;

        private TransactionGetResult(
            [NotNull] string id,
            [NotNull] byte[] content,
            ulong cas,
            [NotNull] ICouchbaseCollection collection,
            TransactionLinks? links,
            TransactionJsonDocumentStatus status,
            DocumentMetadata? documentMetadata,
            ITypeTranscoder transcoder)
        {
            Id = id;
            FullyQualifiedId = GetFullyQualifiedId(collection, id);
            _content = content;
            Cas = cas;
            Collection = collection;
            Links = links;
            Status = status;
            DocumentMetadata = documentMetadata;
            _transcoder = transcoder;
        }

        public TransactionJsonDocumentStatus Status { get; }

        public TransactionLinks? Links { get; }

        public string Id { get; }
        public string FullyQualifiedId { get; }
        public ulong Cas { get; internal set; }
        public DocumentMetadata? DocumentMetadata { get; }
        public ICouchbaseCollection Collection { get; }

        public T ContentAs<T>() => _transcoder.Decode<T>(_content, GetFlags<T>(), OpCode.Get);

        // TODO: not sure about this.
        private Flags GetFlags<T>() => default(T) != null ? _transcoder.GetFormat<T>(default(T)!) : new Flags()
        {
            Compression = Compression.None,
            DataFormat = DataFormat.Json,
            TypeCode = TypeCode.Object
        };

        public static string GetFullyQualifiedId(ICouchbaseCollection collection, string id) =>
            $"{collection.Scope.Bucket.Name}::{collection.Scope.Name}::{collection.Name}::{id}";

        public static TransactionGetResult FromInsert(
            ICouchbaseCollection collection,
            string id,
            byte[] content,
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
            var links = new TransactionLinks(
                Encoding.UTF8.GetString(content),
                atrId,
                atrBucketName,
                atrScopeName,
                atrCollectionName,
                transactionId,
                attemptId,
                null,
                null,
                null,
                "insert"
                );

            return new TransactionGetResult(
                id,
                content,
                updatedDoc.Cas,
                collection,
                links,
                TransactionJsonDocumentStatus.Normal,
                null,
                transcoder
            );
        }

        public static TransactionGetResult FromOther(
            TransactionGetResult doc,
            byte[] content,
            TransactionJsonDocumentStatus status)
        {
            var links = new TransactionLinks(
                doc.Links.StagedContent,
                doc.Links.AtrId,
                doc.Links.AtrBucketName,
                doc.Links.AtrScopeName,
                doc.Links.AtrCollectionName,
                doc.Links.StagedTransactionId,
                doc.Links.StagedAttemptId,
                doc.Links.CasPreTxn,
                doc.Links.RevIdPreTxn,
                doc.Links.ExptimePreTxn,
                doc.Links.Op
            );

            return new TransactionGetResult(
                doc.Id,
                content,
                doc.Cas,
                doc.Collection,
                links,
                status,
                doc.DocumentMetadata,
                doc._transcoder
                );
        }

        public static TransactionGetResult FromLookupIn(
            ICouchbaseCollection collection,
            string id,
            TransactionJsonDocumentStatus status,
            ITypeTranscoder transcoder,
            ulong lookupInCas,
            byte[]? preTxnContent,
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

            var links = stagedContent != null
                ? new TransactionLinks(
                    stagedContent,
                    atrId,
                    atrBucketName,
                    atrScopeName,
                    atrCollectionName,
                    transactionId,
                    attemptId,
                    casPreTxn,
                    revidPreTxn,
                    exptimePreTxn,
                    op)
                : null;

            var dm = new DocumentMetadata()
            {
                // TODO: When data access is refactored, make sure Crc32c is included here.
                Cas = casFromDocument, RevId = revidFromDocument, ExpTime = exptimeFromDocument,
            };

            return new TransactionGetResult(
                id,
                preTxnContent ?? Array.Empty<byte>(),
                lookupInCas,
                collection,
                links,
                status,
                dm,
                transcoder);
        }

    }
}
