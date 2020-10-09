using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.KeyValue;
using Couchbase.Transactions.ActiveTransactionRecords;
using Couchbase.Transactions.Support;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Components
{
    public class DocumentWithTransactionMetadata
    {
        private DocumentWithTransactionMetadata(string? atrId, string? transactionId, string? attemptId, byte[]? stagedData, string? atrBucketName, string? atrCollectionName, string? crc32, DocumentMetadata? transactionRestoreMetadata, string? type, DocumentMetadata? documentMetadata, JObject? preTransactionContent, bool isDeleted, ILookupInResult lookupInResult)
        {
            AtrId = atrId;
            TransactionId = transactionId;
            AttemptId = attemptId;
            StagedData = stagedData;
            AtrBucketName = atrBucketName;
            AtrCollectionName = atrCollectionName;
            Crc32 = crc32;
            TransactionRestoreMetadata = transactionRestoreMetadata;
            Type = type;
            DocumentMetadata = documentMetadata;
            PreTransactionContent = preTransactionContent;
            IsDeleted = isDeleted;
            LookupInResult = lookupInResult;
        }

        [JsonIgnore]
        public ILookupInResult LookupInResult { get; }

        public string? AtrId { get; }
        public string? TransactionId { get; }
        public string? AttemptId { get; }
        public byte[]? StagedData { get; }
        public string? AtrBucketName { get; }
        public string? AtrCollectionName { get; }
        public string? Crc32 { get; }
        public DocumentMetadata? TransactionRestoreMetadata { get; }
        public string? Type { get; }
        public DocumentMetadata? DocumentMetadata { get; }

        public JObject? PreTransactionContent { get;  }

        public bool IsDeleted { get; }

        public static async Task<DocumentWithTransactionMetadata> LookupAsync(ICouchbaseCollection collection, string id, TimeSpan? keyValueTimeout, bool fetchDocBody = true)
        {
            var specs = new List<LookupInSpec>()
            {
                LookupInSpec.Get(TransactionFields.AtrId, true), // 0
                LookupInSpec.Get(TransactionFields.TransactionId, isXattr: true), // 1
                LookupInSpec.Get(TransactionFields.AttemptId, isXattr: true), // 2
                LookupInSpec.Get(TransactionFields.StagedData, isXattr: true), // 3
                LookupInSpec.Get(TransactionFields.AtrBucketName, isXattr: true), // 4
                LookupInSpec.Get(TransactionFields.AtrCollName, isXattr: true), // 5
                LookupInSpec.Get(TransactionFields.TransactionRestorePrefixOnly, true), // 6
                LookupInSpec.Get(TransactionFields.Type, true), // 7
                LookupInSpec.Get(TransactionFields.Crc32, true), // 8
                LookupInSpec.Get("$document", true), // 9
            };

            if (fetchDocBody)
            {
                specs.Add(LookupInSpec.GetFull());
            }

            var doc = await collection.LookupInAsync(
                id, specs,
                opts =>
                    opts.Timeout(keyValueTimeout)
                        .AccessDeleted(true)
            ).CAF();

            DocumentMetadata? docMeta = null;
            if (doc.Exists(9))
            {
                var jobj = doc.ContentAs<JObject>(9);
                docMeta = jobj.ToObject<Components.DocumentMetadata>();
            }

            DocumentMetadata? restoreMeta = null;
            if (doc.Exists(6))
            {
                var jobj = doc.ContentAs<JObject>(6);
                restoreMeta = jobj.ToObject<Components.DocumentMetadata>();
            }

            JObject? preTransactionContent = null;
            if (fetchDocBody && !doc.IsDeleted && doc.Exists(9))
            {
                preTransactionContent = doc.ContentAs<JObject>(9);
            }

            var result = new DocumentWithTransactionMetadata(
                atrId: StringIfExists(doc, 0),
                transactionId: StringIfExists(doc, 1),
                attemptId: StringIfExists(doc, 2),
                stagedData: doc.Exists(3) ? doc.ContentAs<byte[]>(3) : null,
                atrBucketName: StringIfExists(doc, 4),
                atrCollectionName: StringIfExists(doc, 5),
                crc32: StringIfExists(doc, 8),
                transactionRestoreMetadata: restoreMeta,
                documentMetadata: docMeta,
                type: StringIfExists(doc, 7),
                preTransactionContent: preTransactionContent,
                isDeleted: doc.IsDeleted,
                lookupInResult: doc
                );

            return result;
        }

        private static string? StringIfExists(ILookupInResult doc, int index) => doc.Exists(index) ? doc.ContentAs<string>(index) : null;

    }
}
