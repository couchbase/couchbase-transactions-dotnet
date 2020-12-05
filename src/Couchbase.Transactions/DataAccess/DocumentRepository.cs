using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Internal;
using Couchbase.Transactions.Support;

namespace Couchbase.Transactions.DataAccess
{
    internal class DocumentRepository : IDocumentRepository
    {
        private readonly TransactionContext _overallContext;
        private readonly TimeSpan? _keyValueTimeout;
        private readonly DurabilityLevel _durability;
        private readonly string _attemptId;

        public DocumentRepository(TransactionContext overallContext, TimeSpan? keyValueTimeout, DurabilityLevel durability, string attemptId)
        {
            _overallContext = overallContext;
            _keyValueTimeout = keyValueTimeout;
            _durability = durability;
            _attemptId = attemptId;
        }

        public async Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedInsert(ICouchbaseCollection collection, string docId, object content, IAtrRepository atr, ulong? cas = null)
        {
            List<MutateInSpec> specs = CreateMutationOps(atr, "insert", content);
            var opts = GetMutateInOptions(StoreSemantics.Insert)
                .AccessDeleted(true)
                .CreateAsDeleted(true);

            if (cas.HasValue)
            {
                opts.Cas(cas.Value).StoreSemantics(StoreSemantics.Replace);
            }

            var mutateResult = await collection.MutateInAsync(docId, specs, opts).CAF();
            return (mutateResult.Cas, mutateResult.MutationToken);
        }

        public async Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedReplace(TransactionGetResult doc, object content, IAtrRepository atr)
        {
            if (doc.Cas == 0)
            {
                throw new ArgumentOutOfRangeException("Document CAS should not be wildcard or default when replacing.");
            }

            var specs = CreateMutationOps(atr, "replace", content, doc.DocumentMetadata);
            var opts = GetMutateInOptions(StoreSemantics.Replace).Cas(doc.Cas);

            try
            {
                var updatedDoc = await doc.Collection.MutateInAsync(doc.Id, specs, opts).CAF();
                return (updatedDoc.Cas, updatedDoc.MutationToken);
            }
            catch (Couchbase.Core.Exceptions.InvalidArgumentException ex)
            {
                throw;
            }
        }

        public async Task<(ulong updatedCas, MutationToken mutationToken)> MutateStagedRemove(TransactionGetResult doc, IAtrRepository atr)
        {
            var specs = CreateMutationOps(atr, "remove", TransactionFields.StagedDataRemoveKeyword, doc.DocumentMetadata);
            var opts = GetMutateInOptions(StoreSemantics.Replace).Cas(doc.Cas).CreateAsDeleted(true);

            var updatedDoc = await doc.Collection.MutateInAsync(doc.Id, specs, opts).CAF();
            return (updatedDoc.Cas, updatedDoc.MutationToken);
        }

        public async Task<(ulong updatedCas, MutationToken? mutationToken)> UnstageInsertOrReplace(ICouchbaseCollection collection, string docId, ulong cas, object finalDoc, bool insertMode)
        {
            if (insertMode)
            {
                // TODO: set up all these options in the constructor, instead of new'ing them up each time.
                var opts = new InsertOptions().Durability(_durability);
                if (_keyValueTimeout.HasValue)
                {
                    opts.Timeout(_keyValueTimeout.Value);
                }

                var mutateResult = await collection.InsertAsync(docId, finalDoc, opts).CAF();
                return (mutateResult.Cas, mutateResult?.MutationToken);
            }
            else
            {
                var opts = GetMutateInOptions(StoreSemantics.Replace).Cas(cas);
                var mutateResult = await collection.MutateInAsync(docId, specs =>
                            specs.Upsert(TransactionFields.TransactionInterfacePrefixOnly, string.Empty,
                                    isXattr: true, createPath: true)
                                .Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                                .SetDoc(finalDoc)).CAF();
                return (mutateResult.Cas, mutateResult?.MutationToken);
            }
        }

        public async Task UnstageRemove(ICouchbaseCollection collection, string docId)
        {
            var opts = new RemoveOptions().Cas(0).Durability(_durability);
            if (_keyValueTimeout.HasValue)
            {
                opts.Timeout(_keyValueTimeout.Value);
            }

            await collection.RemoveAsync(docId, opts).CAF();
        }

        public async Task ClearTransactionMetadata(ICouchbaseCollection collection, string docId, ulong cas)
        {
            var opts = GetMutateInOptions(StoreSemantics.Replace).AccessDeleted(true).Cas(cas);

            var specs = new MutateInSpec[]
            {
                        MutateInSpec.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null, isXattr: true),
                        MutateInSpec.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
            };

            _ = await collection.MutateInAsync(docId,
                specs, opts).CAF();
        }

        public async Task<DocumentLookupResult> LookupDocumentAsync(ICouchbaseCollection collection, string docId, bool fullDocument = true) => await LookupDocumentAsync(collection, docId, _keyValueTimeout, fullDocument);
        internal static async Task<DocumentLookupResult> LookupDocumentAsync(ICouchbaseCollection collection, string docId, TimeSpan? keyValueTimeout, bool fullDocument = true)
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

            ILookupInResult lookupInResult;
            try
            {
                lookupInResult = await collection.LookupInAsync(docId, specs, opts =>
                    opts.AccessDeleted(true).Timeout(keyValueTimeout)).CAF();
            }
            catch (PathInvalidException pathInvalid)
            {
                // TODO:  Possible NCBC fix needed?  LookupIn with AccessDeleted maybe should not be throwing PathInvalid
                //        i.e. should gracefully handle SUBDOC_MULTI_PATH_FAILURE_DELETED
                if (fullDocIndex != null)
                {
                    specs.RemoveAt(fullDocIndex.Value);
                    fullDocIndex = null;
                    lookupInResult = await collection.LookupInAsync(docId, specs, opts =>
                        opts.AccessDeleted(true).Timeout(keyValueTimeout)).CAF();
                }
                else
                {
                    throw;
                }
            }

            var docMeta = lookupInResult.ContentAs<DocumentMetadata>(1);

            IContentAsWrapper? unstagedContent = fullDocIndex.HasValue
                ? new LookupInContentAsWrapper(lookupInResult, fullDocIndex.Value)
                : null;

            var stagedContent = lookupInResult.Exists(2)
                ? new LookupInContentAsWrapper(lookupInResult, 2)
                : null;

            var result = new DocumentLookupResult(docId,
                unstagedContent,
                stagedContent,
                lookupInResult,
                docMeta,
                collection);

            if (lookupInResult.Exists(0))
            {
                result.TransactionXattrs = lookupInResult.ContentAs<TransactionXattrs>(0);
            }

            return result;
        }

        private MutateInOptions GetMutateInOptions(StoreSemantics storeSemantics) => new MutateInOptions()
                .Timeout(_keyValueTimeout)
                .StoreSemantics(storeSemantics)
                .Durability(_durability);

        private List<MutateInSpec> CreateMutationOps(IAtrRepository atr, string opType, object content, DocumentMetadata? dm = null)
        {
            var specs = new List<MutateInSpec>
            {
                MutateInSpec.Upsert(TransactionFields.TransactionId, _overallContext.TransactionId,
                    createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AttemptId, _attemptId, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrId, atr.AtrId, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrScopeName, atr.ScopeName, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrBucketName, atr.BucketName, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrCollName, atr.CollectionName, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.Type, opType, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.Crc32, MutationMacro.ValueCRC32c, createPath: true, isXattr: true),
            };

            switch (opType)
            {
                case "remove":
                    // FIXME:  why does this fail?  (with or without the UPSERT)
                    ////specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData, string.Empty, createPath: true, isXattr: true));
                    ////specs.Add(MutateInSpec.Remove(TransactionFields.StagedData, isXattr: true));
                    break;
                case "replace":
                case "insert":
                    specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData, content, createPath: true, isXattr: true));
                    break;
            }

            if (dm != null)
            {
                if (dm.Cas != null)
                {
                    specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnCas, dm.Cas, createPath: true, isXattr: true));
                }

                if (dm.RevId != null)
                {
                    specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnRevid, dm.RevId, createPath: true, isXattr: true));
                }

                if (dm.ExpTime != null)
                {
                    specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnExptime, dm.ExpTime, createPath: true, isXattr: true));
                }
            }

            return specs;
        }

    }
}
