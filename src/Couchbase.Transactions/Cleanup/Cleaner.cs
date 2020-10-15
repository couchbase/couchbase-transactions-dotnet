using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Cleanup
{
    internal class Cleaner
    {
        public ICleanupTestHooks TestHooks { get; set; } = DefaultCleanupTestHooks.Instance;
        public static readonly Task NothingToDo = Task.CompletedTask;

        private readonly ICluster _cluster;
        private readonly TimeSpan? _keyValueTimeout;
        private readonly ITypeTranscoder _transcoder;
        public Cleaner(ICluster cluster, TimeSpan? keyValueTimeout, ITypeTranscoder transcoder)
        {
            _cluster = cluster;
            _keyValueTimeout = keyValueTimeout;
            _transcoder = transcoder;
        }


        public async Task ProcessCleanupRequest(CleanupRequest cleanupRequest)
        {
            // TODO: ForwardCompatibilityCheck()
            await CleanupDocs(cleanupRequest).CAF();
            await CleanupAtrEntry(cleanupRequest).CAF();
        }

        private Task CleanupDocs(CleanupRequest cleanupRequest) => cleanupRequest.State switch
        {
            AttemptStates.NOTHING_WRITTEN => NothingToDo,
            AttemptStates.PENDING => NothingToDo,
            AttemptStates.ABORTED => CleanupDocsAborted(cleanupRequest),
            AttemptStates.COMMITTED => CleanupDocsCommitted(cleanupRequest),
            AttemptStates.COMPLETED => NothingToDo,
            AttemptStates.ROLLED_BACK => NothingToDo,
            _ => throw new NotImplementedException(cleanupRequest.State + " Attempt cannot be cleaned up")
        };

        private async Task CleanupAtrEntry(CleanupRequest cleanupRequest)
        {
            try
            {
                await TestHooks.BeforeAtrRemove(cleanupRequest.AtrId).CAF();
                var prefix = $"{TransactionFields.AtrFieldAttempts}.{cleanupRequest.AttemptId}";
                var specs = cleanupRequest.State switch
                {
                    AttemptStates.PENDING => MutateInSpec.Insert(
                        $"{prefix}.{TransactionFields.AtrFieldPendingSentinel}",
                        0, isXattr: true),
                    _ => MutateInSpec.Remove(prefix, isXattr: true)
                };

                await cleanupRequest.AtrCollection.MutateInAsync(cleanupRequest.AtrId, new[] { specs },
                    opts => opts.Timeout(_keyValueTimeout));
            }
            catch (Exception ex)
            {
                var ec = ex.Classify();
                if (ec == ErrorClass.FailPathNotFound)
                {
                    return;
                }

                throw;
            }
        }

        private async Task CleanupDocsAborted(CleanupRequest cleanupRequest)
        {
            foreach (var dr in cleanupRequest.InsertedIds)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: false, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        await TestHooks.BeforeRemoveDoc(dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        var finalDoc = op.StagedContent!.ContentAs<object>();
                        if (op.IsDeleted)
                        {
                            await collection.MutateInAsync(dr.Id, specs =>
                                    specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                                        .SetDoc(finalDoc),
                                opts => opts.Cas(op.Cas)
                                    .CreateAsDeleted(true)
                                    .Timeout(_keyValueTimeout));
                        }
                        else
                        {
                            await collection.RemoveAsync(dr.Id, opts => opts.Cas(op.Cas)
                                .Timeout(_keyValueTimeout)).CAF();
                        }
                    }).CAF();
            }

            var replacedOrRemoved = cleanupRequest.ReplacedIds.Concat(cleanupRequest.RemovedIds);
            foreach (var dr in replacedOrRemoved)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: false, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        await TestHooks.BeforeRemoveLinks(dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        var finalDoc = op.StagedContent!.ContentAs<object>();
                        await collection.MutateInAsync(dr.Id, specs =>
                                specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true),
                            opts => opts.Cas(op.Cas)
                                .CreateAsDeleted(true)
                                .Timeout(_keyValueTimeout));
                    }).CAF();
            }
        }

        private async Task CleanupDocsCommitted(CleanupRequest cleanupRequest)
        {
            var insertedOrReplaced = cleanupRequest.InsertedIds.Concat(cleanupRequest.ReplacedIds);
            foreach (var dr in insertedOrReplaced)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: true, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        // TODO: This has significant overlap with UnstageInsertOrReplace.
                        await TestHooks.BeforeCommitDoc(dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        var finalDoc = op.StagedContent!.ContentAs<object>();
                        if (op.IsDeleted)
                        {
                            await collection.InsertAsync(dr.Id, finalDoc).CAF();
                        }
                        else
                        {
                            // TODO: spec says, "set accessDeleted flag". Is that a typo and they mean CreateAsDeleted?
                            await collection.MutateInAsync(dr.Id, specs =>
                                    specs.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                                        .SetDoc(finalDoc)
                                , opts => opts.Cas(op.Cas)
                                    .CreateAsDeleted(true)
                                    .Timeout(_keyValueTimeout)).CAF();
                        }
                    }).CAF();
            }

            foreach (var dr in cleanupRequest.RemovedIds)
            {
                await CleanupDoc(dr, requireCrc32ToMatchStaging: true, attemptId: cleanupRequest.AttemptId,
                    perDoc: async (op) =>
                    {
                        await TestHooks.BeforeRemoveDocStagedForRemoval(dr.Id).CAF();
                        var collection = await dr.GetCollection(_cluster).CAF();
                        await collection.RemoveAsync(dr.Id, opts => opts.Cas(op.Cas)
                            .Timeout(_keyValueTimeout)).CAF();
                    }).CAF();
            }
        }

        public async Task CleanupDoc(DocRecord dr, bool requireCrc32ToMatchStaging, Func<DocumentLookupResult, Task> perDoc, string attemptId)
        {
            try
            {
                await TestHooks.BeforeDocGet(dr.Id).CAF();
                var collection = await dr.GetCollection(_cluster).CAF();
                var docLookupResult = await DocumentLookupResult
                    .LookupDocumentAsync(collection, dr.Id, _keyValueTimeout, fullDocument: false).CAF();

                if (docLookupResult.TransactionXattrs == null)
                {
                    return;
                }

                if (docLookupResult.TransactionXattrs.Id?.AttemptId != attemptId)
                {
                    return;
                }

                if (requireCrc32ToMatchStaging && !string.IsNullOrEmpty(docLookupResult.DocumentMetadata?.Crc32c))
                {
                    if (docLookupResult.DocumentMetadata?.Crc32c != docLookupResult.TransactionXattrs.Operation?.Crc32)
                    {
                        // "the world has moved on", continue as success
                        return;
                    }
                }

                // If we reach here, the document is unchanged from staging, and it's safe to proceed
                await perDoc(docLookupResult).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // TODO: how to record cleanup failures? ...
                throw;
            }
        }
    }
}
