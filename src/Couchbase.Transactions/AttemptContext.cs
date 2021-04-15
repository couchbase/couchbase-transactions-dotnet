using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Couchbase.Core;
using Couchbase.Core.Exceptions;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Core.IO.Operations;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Core.Logging;
using Couchbase.KeyValue;
using Couchbase.Transactions.ActiveTransactionRecords;
using Couchbase.Transactions.Cleanup;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.DataAccess;
using Couchbase.Transactions.DataModel;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.Attempts;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Error.Internal;
using Couchbase.Transactions.Forwards;
using Couchbase.Transactions.Internal;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;
using DnsClient.Internal;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static Couchbase.Transactions.Error.ErrorBuilder;
using Exception = System.Exception;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;

namespace Couchbase.Transactions
{
    public class AttemptContext
    {
        private static readonly TimeSpan WriteWriteConflictTimeLimit = TimeSpan.FromSeconds(1);
        private readonly TransactionContext _overallContext;
        private readonly TransactionConfig _config;
        private readonly ITestHooks _testHooks;
        internal IRedactor Redactor { get; }
        private AttemptStates _state = AttemptStates.NOTHING_WRITTEN;
        private readonly ErrorTriage _triage;

        private readonly List<StagedMutation> _stagedMutations = new List<StagedMutation>();
        private readonly object _initAtrLock = new object();
        private IAtrRepository? _atr = null;
        private IDocumentRepository _docs;
        private readonly DurabilityLevel _effectiveDurabilityLevel;
        private readonly List<MutationToken> _finalMutations = new List<MutationToken>();
        private readonly ConcurrentDictionary<long, TransactionOperationFailedException> _previousErrors = new ConcurrentDictionary<long, TransactionOperationFailedException>();
        private bool _expirationOvertimeMode = false;

        public string AttemptId { get; }
        public string TransactionId => _overallContext.TransactionId;

        internal bool UnstagingComplete { get; private set; } = false;

        internal AttemptContext(TransactionContext overallContext,
            TransactionConfig config,
            string attemptId,
            ITestHooks? testHooks,
            IRedactor redactor,
            Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null,
            IDocumentRepository? documentRepository = null,
            IAtrRepository? atrRepository = null)
        {
            AttemptId = attemptId ?? throw new ArgumentNullException(nameof(attemptId));
            _overallContext = overallContext ?? throw new ArgumentNullException(nameof(overallContext));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _testHooks = testHooks ?? DefaultTestHooks.Instance;
            Redactor = redactor ?? throw new ArgumentNullException(nameof(redactor));
            _effectiveDurabilityLevel = _overallContext.PerConfig?.DurabilityLevel ?? config.DurabilityLevel;
            Logger = loggerFactory?.CreateLogger<AttemptContext>();
            _triage = new ErrorTriage(this, loggerFactory);
            _docs = documentRepository ?? new DocumentRepository(_overallContext, _config.KeyValueTimeout, _effectiveDurabilityLevel, AttemptId);
            if (atrRepository != null)
            {
                _atr = atrRepository;
            }
        }

        public ILogger<AttemptContext>? Logger { get; }

        /// <summary>
        /// Gets a document.
        /// </summary>
        /// <param name="collection">The collection to look up the document in.</param>
        /// <param name="id">The ID of the document.</param>
        /// <returns>A <see cref="TransactionGetResult"/> containing the document.</returns>
        /// <exception cref="DocumentNotFoundException">If the document does not exist.</exception>
        public async Task<TransactionGetResult> GetAsync(ICouchbaseCollection collection, string id)
        {
            var getResult = await GetOptionalAsync(collection, id).CAF();
            if (getResult == null)
            {
                throw new DocumentNotFoundException();
            }

            return getResult;
        }

        /// <summary>
        /// Gets a document or null.
        /// </summary>
        /// <param name="collection">The collection to look up the document in.</param>
        /// <param name="id">The ID of the document.</param>
        /// <returns>A <see cref="TransactionGetResult"/> containing the document, or null if  not found.</returns>
        public async Task<TransactionGetResult?> GetOptionalAsync(ICouchbaseCollection collection, string id)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiryAndThrow(id, hookPoint: ITestHooks.HOOK_GET);

            /*
             * Check stagedMutations.
               If the doc already exists in there as a REPLACE or INSERT return its post-transaction content in a TransactionGetResult.
                Protocol 2.0 amendment: and TransactionGetResult::links().isDeleted() reflecting whether it is a tombstone or not.
               Else if the doc already exists in there as a remove, return empty.
             */
            var staged = FindStaged(collection, id);
            if (staged != null)
            {
                switch (staged.Type)
                {
                    case StagedMutationType.Insert:
                    case StagedMutationType.Replace:
                        // LOGGER.info(attemptId, "found own-write of mutated doc %s", RedactableArgument.redactUser(id));
                        return TransactionGetResult.FromOther(staged.Doc, new JObjectContentWrapper(staged.Content), TransactionJsonDocumentStatus.OwnWrite);
                    case StagedMutationType.Remove:
                        // LOGGER.info(attemptId, "found own-write of removed doc %s", RedactableArgument.redactUser(id));
                        return null;
                    default:
                        throw new InvalidOperationException($"Document '{Redactor.UserData(id)}' was staged with type {staged.Type}");
                }
            }

            try
            {
                try
                {
                    await _testHooks.BeforeDocGet(this, id).CAF();

                    var result = await GetWithMavAsync(collection, id);

                    await _testHooks.AfterGetComplete(this, id).CAF();
                    await ForwardCompatibility.Check(this, ForwardCompatibility.Gets, result?.TransactionXattrs?.ForwardCompatibility);
                    return result;
                }
                catch (Exception ex)
                {
                    var tr = _triage.TriageGetErrors(ex);
                    switch (tr.ec)
                    {
                        case ErrorClass.FailDocNotFound:
                            return TransactionGetResult.Empty;
                        default:
                            throw _triage.AssertNotNull(tr, ex);
                    }
                }
            }
            catch (TransactionOperationFailedException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        private async Task<TransactionGetResult?> GetWithMavAsync(ICouchbaseCollection collection, string id, string? resolveMissingAtrEntry = null)
        {
            try
            {
                // we need to resolve the state of that transaction. Here is where we do the “Monotonic Atomic View” (MAV) logic
                try
                {
                    // Do a Sub-Document lookup, getting all transactional metadata, the “$document” virtual xattr,
                    // and the document’s body. Timeout is set as in Timeouts.
                    var docLookupResult = await _docs.LookupDocumentAsync(collection, id, fullDocument: true).CAF();
                    Logger?.LogDebug($"{nameof(GetWithMavAsync)} for {Redactor.UserData(id)}, attemptId={AttemptId}, postCas={docLookupResult.Cas}");

                    if (docLookupResult == null)
                    {
                        return TransactionGetResult.Empty;
                    }

                    var txn = docLookupResult?.TransactionXattrs;
                    if (txn?.Id?.AttemptId == null
                        || txn?.Id?.Transactionid == null
                        || txn?.AtrRef?.BucketName == null
                        || txn?.AtrRef?.CollectionName == null)
                    {
                        // Not in a transaction, or insufficient transaction metadata
                        return docLookupResult!.IsDeleted
                            ? TransactionGetResult.Empty
                            : docLookupResult.GetPreTransactionResult(TransactionJsonDocumentStatus.Normal);
                    }

                    if (resolveMissingAtrEntry == txn.Id?.AttemptId)
                    {
                        // This is our second attempt getting the document, and it’s in the same state as before
                        return docLookupResult!.IsDeleted
                            ? TransactionGetResult.Empty
                            : docLookupResult.GetPostTransactionResult(TransactionJsonDocumentStatus.InTxnOther);
                    }

                    resolveMissingAtrEntry = txn.Id?.AttemptId;

                    // TODO: double-check if atr attemptid == this attempt id, and return post-transaction version
                    // (should have been covered by staged mutation check)

                    var getCollectionTask = _atr?.GetAtrCollection(txn.AtrRef)
                                            ?? AtrRepository.GetAtrCollection(txn.AtrRef, collection);
                    var docAtrCollection = await getCollectionTask.CAF()
                                           ?? throw new ActiveTransactionRecordNotFoundException();

                    var findEntryTask = _atr?.FindEntryForTransaction(docAtrCollection, txn.AtrRef.Id!, txn.Id!.AttemptId)
                        ?? AtrRepository.FindEntryForTransaction(docAtrCollection, txn.AtrRef.Id!, txn.Id!.AttemptId, _config.KeyValueTimeout);
                    var atrEntry = await findEntryTask.CAF()
                                   ?? throw new ActiveTransactionRecordEntryNotFoundException();

                    if (txn.Id!.AttemptId == AttemptId)
                    {
                        if (txn.Operation?.Type == "remove")
                        {
                            return TransactionGetResult.Empty;
                        }
                        else
                        {
                            return docLookupResult!.GetPostTransactionResult(TransactionJsonDocumentStatus.OwnWrite);
                        }
                    }

                    await ForwardCompatibility.Check(this, ForwardCompatibility.GetsReadingAtr, atrEntry.ForwardCompatibility);

                    if (atrEntry.State == AttemptStates.COMMITTED || atrEntry.State == AttemptStates.COMPLETED)
                    {
                        if (txn.Operation?.Type == "remove")
                        {
                            return TransactionGetResult.Empty;
                        }

                        return docLookupResult!.GetPostTransactionResult(TransactionJsonDocumentStatus.InTxnCommitted);
                    }

                    if (docLookupResult!.IsDeleted || txn.Operation?.Type == "insert")
                    {
                        return TransactionGetResult.Empty;
                    }

                    return docLookupResult.GetPreTransactionResult(TransactionJsonDocumentStatus.InTxnOther);
                }
                catch (ActiveTransactionRecordEntryNotFoundException)
                {
                    throw;
                }
                catch (Exception atrLookupException)
                {
                    var atrLookupTriage = _triage.TriageAtrLookupInMavErrors(atrLookupException);
                    throw _triage.AssertNotNull(atrLookupTriage, atrLookupException);
                }
            }
            catch (ActiveTransactionRecordEntryNotFoundException ex)
            {
                Logger.LogWarning("ATR entry not found: {ex}", ex);
                if (resolveMissingAtrEntry == null)
                {
                    throw;
                }

                return await GetWithMavAsync(collection, id, resolveMissingAtrEntry).CAF();
            }
        }

        private void CheckErrors()
        {
            /*
             * Before performing any operation, including commit, check if the errors member is non-empty.
             * If so, raise an Error(ec=FAIL_OTHER, cause=PreviousOperationFailed).
             */
            if (!_previousErrors.IsEmpty)
            {
                throw ErrorBuilder.CreateError(this, ErrorClass.FailOther)
                    .Cause(new PreviousOperationFailedException(_previousErrors.Values))
                    .Build();
            }
        }

        private StagedMutation FindStaged(ICouchbaseCollection collection, string id)
        {
            return _stagedMutations.Find(sm => sm.Doc.Id == id
                                               && sm.Doc.Collection.Name == collection.Name
                                               && sm.Doc.Collection.Scope.Name == collection.Scope.Name
                                               && sm.Doc.Collection.Scope.Bucket.Name == collection.Scope.Bucket.Name);
        }

        private StagedMutation FindStaged(TransactionGetResult doc) => FindStaged(doc.Collection, doc.Id);

        /// <summary>
        /// Replace the content of a document previously fetched in this transaction with new content.
        /// </summary>
        /// <param name="doc">The <see cref="TransactionGetResult"/> of a document previously looked up in this transaction.</param>
        /// <param name="content">The updated content.</param>
        /// <returns>A <see cref="TransactionGetResult"/> reflecting the updated content.</returns>
        public async Task<TransactionGetResult> ReplaceAsync(TransactionGetResult doc, object content)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiryAndThrow(doc.Id, ITestHooks.HOOK_REPLACE);
            await CheckWriteWriteConflict(doc, ForwardCompatibility.WriteWriteConflictReplacing).CAF();
            await InitAtrIfNeeded(doc.Collection, doc.Id);
            await SetAtrPendingIfFirstMutation(doc.Collection);

            return await CreateStagedReplace(doc, content, accessDeleted: doc.IsDeleted);
        }

        private async Task SetAtrPendingIfFirstMutation(ICouchbaseCollection collection)
        {
            if (_stagedMutations.Count == 0)
            {
                await SetAtrPending();
            }
        }

        private async Task<TransactionGetResult> CreateStagedReplace(TransactionGetResult doc, object content, bool accessDeleted)
        {
            _ = _atr ?? throw new ArgumentNullException(nameof(_atr), "ATR should have already been initialized");
            try
            {
                try
                {
                    await _testHooks.BeforeStagedReplace(this, doc.Id);
                    var contentWrapper = new JObjectContentWrapper(content);
                    bool isTombstone = doc.Cas == 0;
                    (var updatedCas, var mutationToken) = await _docs.MutateStagedReplace(doc, content, _atr, accessDeleted).CAF();
                    Logger?.LogDebug($"{nameof(CreateStagedReplace)} for {Redactor.UserData(doc.Id)}, attemptId={AttemptId}, preCas={doc.Cas}, postCas={updatedCas}, accessDeleted={accessDeleted}");
                    await _testHooks.AfterStagedReplaceComplete(this, doc.Id).CAF();

                    doc.Cas = updatedCas;

                    var stagedOld = FindStaged(doc);
                    if (stagedOld != null)
                    {
                        _stagedMutations.Remove(stagedOld);
                    }

                    if (stagedOld?.Type == StagedMutationType.Insert)
                    {
                        // If doc is already in stagedMutations as an INSERT or INSERT_SHADOW, then remove that, and add this op as a new INSERT or INSERT_SHADOW(depending on what was replaced).
                        _stagedMutations.Add(new StagedMutation(doc, content, StagedMutationType.Insert, mutationToken));
                    }
                    else
                    {
                        // If doc is already in stagedMutations as a REPLACE, then overwrite it.
                        _stagedMutations.Add(new StagedMutation(doc, content, StagedMutationType.Replace, mutationToken));
                    }

                    return TransactionGetResult.FromInsert(
                        doc.Collection,
                        doc.Id,
                        contentWrapper,
                        _overallContext.TransactionId,
                        AttemptId,
                        _atr.AtrId,
                        _atr.BucketName,
                        _atr.ScopeName,
                        _atr.CollectionName,
                        updatedCas,
                        isTombstone);
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageCreateStagedRemoveOrReplaceError(ex);
                    if (triaged.ec == ErrorClass.FailExpiry)
                    {
                        _expirationOvertimeMode = true;
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }
            catch (TransactionOperationFailedException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        /// <summary>
        /// Insert a document.
        /// </summary>
        /// <param name="collection">The collection to insert the document into.</param>
        /// <param name="id">The ID of the new document.</param>
        /// <param name="content">The content of the new document.</param>
        /// <returns>A <see cref="TransactionGetResult"/> representing the inserted document.</returns>
        public async Task<TransactionGetResult> InsertAsync(ICouchbaseCollection collection, string id, object content)
        {
            DoneCheck();
            CheckErrors();

            // If this document already exists in StagedMutation, raise Error(FAIL_OTHER, cause=IllegalStateException [or platform-specific equivalent]).
            if (_stagedMutations.Any(sm => sm.Doc.FullyQualifiedId == TransactionGetResult.GetFullyQualifiedId(collection, id)))
            {
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Document is already staged for a mutation."))
                    .Build();
            }

            CheckExpiryAndThrow(id, hookPoint: ITestHooks.HOOK_INSERT);

            await InitAtrIfNeeded(collection, id);
            await SetAtrPendingIfFirstMutation(collection);


            return await CreateStagedInsert(collection, id, content).CAF();
        }

        private async Task<TransactionGetResult> CreateStagedInsert(ICouchbaseCollection collection, string id, object content, ulong? cas = null)
        {
            try
            {
                bool isTombstone = cas == null;
                var result = await RepeatUntilSuccessOrThrow<TransactionGetResult?>(async () =>
                {
                    try
                    {
                        // Check expiration again, since insert might be retried.
                        ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_CREATE_STAGED_INSERT, id);

                        await _testHooks.BeforeStagedInsert(this, id).CAF();
                        var contentWrapper = new JObjectContentWrapper(content);
                        (var updatedCas, var mutationToken) = await _docs.MutateStagedInsert(collection, id, content, _atr!, cas);
                        Logger?.LogDebug($"{nameof(CreateStagedInsert)} for {Redactor.UserData(id)}, attemptId={AttemptId}, preCas={cas}, postCas={updatedCas}");
                        _ = _atr ?? throw new ArgumentNullException(nameof(_atr), "ATR should have already been initialized");
                        var getResult = TransactionGetResult.FromInsert(
                            collection,
                            id,
                            contentWrapper,
                            _overallContext.TransactionId,
                            AttemptId,
                            _atr.AtrId,
                            _atr.BucketName,
                            _atr.ScopeName,
                            _atr.CollectionName,
                            updatedCas,
                            isTombstone);

                        await _testHooks.AfterStagedInsertComplete(this, id).CAF();

                        var stagedMutation = new StagedMutation(getResult, content, StagedMutationType.Insert,
                            mutationToken);
                        _stagedMutations.Add(stagedMutation);

                        return (RepeatAction.NoRepeat, getResult);
                    }
                    catch (Exception ex)
                    {
                        var triaged = _triage.TriageCreateStagedInsertErrors(ex, _expirationOvertimeMode);
                        switch (triaged.ec)
                        {
                            case ErrorClass.FailExpiry:
                                _expirationOvertimeMode = true;
                                throw _triage.AssertNotNull(triaged, ex);
                            case ErrorClass.FailAmbiguous:
                                return (RepeatAction.RepeatWithDelay, null);
                            case ErrorClass.FailCasMismatch:
                            case ErrorClass.FailDocAlreadyExists:
                                var repeatAction = await RepeatUntilSuccessOrThrow<RepeatAction>(async () =>
                                {
                                    // handle FAIL_DOC_ALREADY_EXISTS
                                    try
                                    {
                                        Logger?.LogDebug($"{nameof(CreateStagedInsert)}.HandleDocExists for {Redactor.UserData(id)}, attemptId={AttemptId}, preCas = 0");
                                        await _testHooks.BeforeGetDocInExistsDuringStagedInsert(this, id).CAF();
                                        var docWithMeta = await _docs.LookupDocumentAsync(collection, id, fullDocument: false).CAF();
                                        Logger?.LogDebug($"{nameof(CreateStagedInsert)}.HandleDocExists for {Redactor.UserData(id)}, attemptId={AttemptId}, postCas={docWithMeta.Cas}");
                                        await ForwardCompatibility.Check(this, ForwardCompatibility.WriteWriteConflictInsertingGet, docWithMeta?.TransactionXattrs?.ForwardCompatibility);

                                        var docInATransaction =
                                            docWithMeta?.TransactionXattrs?.Id?.Transactionid != null;
                                        isTombstone = docWithMeta?.IsDeleted == true;

                                        if (isTombstone && !docInATransaction)
                                        {
                                            // If the doc is a tombstone and not in any transaction
                                            // -> It’s ok to go ahead and overwrite.
                                            // Perform this algorithm (createStagedInsert) from the top with cas=the cas from the get.
                                            cas = docWithMeta!.Cas;

                                            // (innerRepeat, createStagedInsertRepeat)
                                            return (RepeatAction.NoRepeat, RepeatAction.RepeatNoDelay);
                                        }

                                        // Else if the doc is not in a transaction
                                        // -> Raise Error(FAIL_DOC_ALREADY_EXISTS, cause=DocumentExistsException).
                                        // There is logic further up the stack that handles this by fast-failing the transaction.
                                        if (!docInATransaction)
                                        {
                                            throw CreateError(this, ErrorClass.FailDocAlreadyExists)
                                                .Cause(new DocumentExistsException())
                                                .Build();
                                        }
                                        else
                                        {
                                            // Else call the CheckWriteWriteConflict logic, which conveniently does everything we need to handle the above cases.
                                            var getResult = docWithMeta!.GetPostTransactionResult(TransactionJsonDocumentStatus.InTxnOther);
                                            await CheckWriteWriteConflict(getResult, ForwardCompatibility.WriteWriteConflictInserting).CAF();

                                            // If this logic succeeds, we are ok to overwrite the doc.
                                            // Perform this algorithm (createStagedInsert) from the top, with cas=the cas from the get.
                                            cas = docWithMeta.Cas;
                                            return (RepeatAction.NoRepeat, RepeatAction.RepeatNoDelay);
                                        }
                                    }
                                    catch (Exception exDocExists)
                                    {
                                        var triagedDocExists = _triage.TriageDocExistsOnStagedInsertErrors(exDocExists);
                                        throw _triage.AssertNotNull(triagedDocExists, exDocExists);
                                    }
                                }).CAF();

                                return (repeatAction, null);
                        }

                        throw _triage.AssertNotNull(triaged, ex);
                    }
                }).CAF();

                return result ?? throw new InvalidOperationException("Final result should not be null");
            }
            catch (TransactionOperationFailedException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        private IEnumerable<StagedMutation> StagedInserts =>
            _stagedMutations.Where(sm => sm.Type == StagedMutationType.Insert);

        private IEnumerable<StagedMutation> StagedReplaces => _stagedMutations.Where(sm => sm.Type == StagedMutationType.Replace);
        private IEnumerable<StagedMutation> StagedRemoves => _stagedMutations.Where(sm => sm.Type == StagedMutationType.Remove);


        private async Task SetAtrPending()
        {
            var atrId = _atr!.AtrId;

            try
            {
                await RepeatUntilSuccessOrThrow(async () =>
                {
                    try
                    {
                        ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ATR_PENDING);
                        await _testHooks.BeforeAtrPending(this);
                        var t1 = _overallContext.StartTime;
                        var t2 = DateTimeOffset.UtcNow;
                        var tElapsed = t2 - t1;
                        var tc = _config.ExpirationTime;
                        var tRemaining = tc - tElapsed;
                        var exp = (ulong)Math.Max(Math.Min(tRemaining.TotalMilliseconds, tc.TotalMilliseconds), 0);
                        await _atr.MutateAtrPending(exp);
                        Logger?.LogDebug($"{nameof(SetAtrPending)} for {Redactor.UserData(_atr.FullPath)} (attempt={AttemptId})");
                        await _testHooks.AfterAtrPending(this);
                        _state = AttemptStates.PENDING;
                        return RepeatAction.NoRepeat;
                    }
                    catch (Exception ex)
                    {
                        var triaged = _triage.TriageSetAtrPendingErrors(ex, _expirationOvertimeMode);
                        Logger?.LogWarning("Failed with {ec} in {method}: {reason}", triaged.ec, nameof(SetAtrPending), ex.Message);
                        switch (triaged.ec)
                        {
                            case ErrorClass.FailExpiry:
                                _expirationOvertimeMode = true;
                                break;
                            case ErrorClass.FailAmbiguous:
                                return RepeatAction.RepeatWithDelay;
                            case ErrorClass.FailPathAlreadyExists:
                                // proceed as though op was successful.
                                return RepeatAction.NoRepeat;
                        }

                        throw _triage.AssertNotNull(triaged, ex);
                    }
                }).CAF();
            }
            catch (TransactionOperationFailedException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        /// <summary>
        /// Remove a document previously looked up in this transaction.
        /// </summary>
        /// <param name="doc">The <see cref="TransactionGetResult"/> of a document previously looked up in this transaction.</param>
        /// <returns>A task representing the asynchronous work.</returns>
        public async Task RemoveAsync(TransactionGetResult doc)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiryAndThrow(doc.Id, ITestHooks.HOOK_REMOVE);
            if (StagedInserts.Any(sm => sm.Doc.FullyQualifiedId == doc.FullyQualifiedId))
            {
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Document is already staged for insert."))
                    .Build();
            }

            await CheckWriteWriteConflict(doc, ForwardCompatibility.WriteWriteConflictRemoving).CAF();
            await InitAtrIfNeeded(doc.Collection, doc.Id);
            await SetAtrPendingIfFirstMutation(doc.Collection).CAF();
            await CreateStagedRemove(doc).CAF();
        }

        private async Task CreateStagedRemove(TransactionGetResult doc)
        {
            try
            {
                try
                {
                    await _testHooks.BeforeStagedRemove(this, doc.Id).CAF();
                    (var updatedCas, var mutationToken) = await _docs.MutateStagedRemove(doc, _atr!).CAF();
                    Logger?.LogDebug($"{nameof(CreateStagedRemove)} for {Redactor.UserData(doc.Id)}, attemptId={AttemptId}, preCas={doc.Cas}, postCas={updatedCas}");
                    await _testHooks.AfterStagedRemoveComplete(this, doc.Id).CAF();

                    doc.Cas = updatedCas;
                    if (_stagedMutations.Exists(sm => sm.Doc.Id == doc.Id && sm.Type == StagedMutationType.Insert))
                    {
                        // TXNJ-35: handle insert-delete with same doc

                        // CommitAsync+rollback: Want to delete the staged empty doc
                        // However this is hard in practice.  If we remove from stagedInsert and add to
                        // stagedRemove then commit will work fine, but rollback will not remove the doc.
                        // So, fast fail this scenario.
                        throw new InvalidOperationException(
                            $"doc {Redactor.UserData(doc.Id)} is being removed after being inserted in the same txn.");
                    }

                    var stagedRemove = new StagedMutation(doc, TransactionFields.StagedDataRemoveKeyword,
                        StagedMutationType.Remove, mutationToken);
                    _stagedMutations.Add(stagedRemove);
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageCreateStagedRemoveOrReplaceError(ex);
                    if (triaged.ec == ErrorClass.FailExpiry)
                    {
                        _expirationOvertimeMode = true;
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }
            catch (TransactionOperationFailedException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        internal async Task AutoCommit()
        {
            switch (_state)
            {
                case AttemptStates.NOTHING_WRITTEN:
                case AttemptStates.PENDING:
                    await CommitAsync().CAF();
                    break;
            }
        }

        public async Task CommitAsync()
        {
            if (!_previousErrors.IsEmpty)
            {
                _triage.ThrowIfCommitWithPreviousErrors(_previousErrors.Values);
            }

            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#CommitAsync
            CheckExpiryAndThrow(null, ITestHooks.HOOK_BEFORE_COMMIT);
            DoneCheck();
            if (_stagedMutations.Count ==  0)
            {
                // If no mutation has been performed. Return success.
                // This will leave state as NOTHING_WRITTEN,
                return;
            }

            await SetAtrCommit().CAF();
            await UnstageDocs().CAF();
            await SetAtrComplete().CAF();
        }

        private async Task SetAtrComplete()
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#SetATRComplete
            if (HasExpiredClientSide(null, ITestHooks.HOOK_ATR_COMPLETE) && !_expirationOvertimeMode)
            {
                // If transaction has expired and not in ExpiryOvertimeMode: though technically expired, the transaction should be regarded
                // as successful, as this is just a cleanup step.
                // Return success.
                return;
            }

            try
            {
                await _testHooks.BeforeAtrComplete(this).CAF();
                await _atr!.MutateAtrComplete().CAF();
                Logger?.LogDebug($"{nameof(SetAtrComplete)} for {Redactor.UserData(_atr.FullPath)} (attempt={AttemptId})");
                await _testHooks.AfterAtrComplete(this).CAF();
                _state = AttemptStates.COMPLETED;
                UnstagingComplete = true;
            }
            catch (Exception ex)
            {
                var triaged = _triage.TriageSetAtrCompleteErrors(ex);
                if (triaged.toThrow != null)
                {
                    throw triaged.toThrow;
                }
                else
                {
                    // Else -> Setting the ATR to COMPLETED is purely a cleanup step, there’s no need to retry it until expiry.
                    // Simply return success (leaving state at COMMITTED).
                    return;
                }
            }
        }

        private async Task UnstageDocs()
        {
            foreach (var sm in _stagedMutations)
            {
                (var cas, var content) = await FetchIfNeededBeforeUnstage(sm).CAF();
                switch (sm.Type)
                {
                    case StagedMutationType.Remove:
                        await UnstageRemove(sm).CAF();
                        break;
                    case StagedMutationType.Insert:
                        await UnstageInsertOrReplace(sm, cas, content, insertMode: true, ambiguityResolutionMode: false).CAF();
                        break;
                    case StagedMutationType.Replace:
                        await UnstageInsertOrReplace(sm, cas, content, insertMode: false, ambiguityResolutionMode: false).CAF();
                        break;
                    default:
                        throw new InvalidOperationException($"Cannot un-stage transaction mutation of type {sm.Type}");
                }
            }
        }

        private async Task UnstageRemove(StagedMutation sm, bool ambiguityResolutionMode = false)
        {
            // TODO: Updated spec.
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Removes
            int retryCount = -1;
            await RepeatUntilSuccessOrThrow(async () =>
            {
                retryCount++;
                try
                {
                    await _testHooks.BeforeDocRemoved(this, sm.Doc.Id).CAF();
                    if (!_expirationOvertimeMode && HasExpiredClientSide(sm.Doc.Id, ITestHooks.HOOK_REMOVE_DOC))
                    {
                        _expirationOvertimeMode = true;
                    }

                    await _docs.UnstageRemove(sm.Doc.Collection, sm.Doc.Id).CAF();
                    Logger?.LogDebug(
                        $"Unstaged RemoveAsync successfully for {Redactor.UserData(sm.Doc.FullyQualifiedId)} (retryCount={retryCount}");
                    await _testHooks.AfterDocRemovedPreRetry(this, sm.Doc.Id).CAF();

                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageUnstageRemoveErrors(ex, _expirationOvertimeMode);
                    if (_expirationOvertimeMode)
                    {
                        throw ErrorBuilder.CreateError(this, ErrorClass.FailExpiry, new AttemptExpiredException(this))
                            .DoNotRollbackAttempt()
                            .RaiseException(TransactionOperationFailedException.FinalError.TransactionFailedPostCommit)
                            .Build();
                    }

                    switch (triaged.ec)
                    {
                        case ErrorClass.FailAmbiguous:
                            ambiguityResolutionMode = true;
                            return RepeatAction.RepeatWithDelay;
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }).CAF();

            _finalMutations.Add(sm.MutationToken);
            await _testHooks.AfterDocRemovedPostRetry(this, sm.Doc.Id).CAF();
        }

        private Task<(ulong cas, object content)> FetchIfNeededBeforeUnstage(StagedMutation sm)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#FetchIfNeededBeforeUnstage
            // TODO: consider implementing ExtMemoryOptUnstaging mode
            // For now, assuming ExtTimeOptUnstaging mode...
            return Task.FromResult((sm.Doc.Cas, sm.Content));
        }

        private async Task UnstageInsertOrReplace(StagedMutation sm, ulong cas, object content, bool insertMode = false, bool ambiguityResolutionMode = false)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Inserts-and-Replaces-Protocol-20-version

            await RepeatUntilSuccessOrThrow(async () =>
            {
                try
                {
                    if (!_expirationOvertimeMode && HasExpiredClientSide(sm.Doc.Id, ITestHooks.HOOK_COMMIT_DOC))
                    {
                        _expirationOvertimeMode = true;
                    }

                    await _testHooks.BeforeDocCommitted(this, sm.Doc.Id).CAF();
                    (ulong updatedCas, MutationToken mutationToken) = await _docs.UnstageInsertOrReplace(sm.Doc.Collection, sm.Doc.Id, cas, content, insertMode).CAF();
                    Logger?.LogInformation(
                        $"Unstaged mutation successfully on {Redactor.UserData(sm.Doc.FullyQualifiedId)}, attempt={AttemptId}, insertMode={insertMode}, ambiguityResolutionMode={ambiguityResolutionMode}, preCas={cas}, postCas={updatedCas}");

                    if (mutationToken != null)
                    {
                        _finalMutations.Add(mutationToken);
                    }

                    await _testHooks.AfterDocCommittedBeforeSavingCas(this, sm.Doc.Id).CAF();

                    sm.Doc.Cas = updatedCas;
                    await _testHooks.AfterDocCommitted(this, sm.Doc.Id).CAF();

                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageUnstageInsertOrReplaceErrors(ex, _expirationOvertimeMode);
                    if (_expirationOvertimeMode)
                    {
                        throw ErrorBuilder.CreateError(this, ErrorClass.FailExpiry, new AttemptExpiredException(this))
                            .DoNotRollbackAttempt()
                            .RaiseException(TransactionOperationFailedException.FinalError.TransactionFailedPostCommit)
                            .Build();
                    }

                    switch (triaged.ec)
                    {
                        case ErrorClass.FailAmbiguous:
                            ambiguityResolutionMode = true;
                            return RepeatAction.RepeatWithDelay;
                        case ErrorClass.FailCasMismatch:
                            if (ambiguityResolutionMode)
                            {
                                throw _triage.AssertNotNull(triaged, ex);
                            }
                            else
                            {
                                cas = 0;
                                return RepeatAction.RepeatWithDelay;
                            }
                        case ErrorClass.FailDocNotFound:
                            // TODO: publish IllegalDocumentState event to the application.
                            Logger?.LogError("IllegalDocumentState: " + triaged.ec);
                            insertMode = true;
                            return RepeatAction.RepeatWithDelay;
                        case ErrorClass.FailDocAlreadyExists:
                            if (ambiguityResolutionMode)
                            {
                                throw _triage.AssertNotNull(triaged, ex);
                            }
                            else
                            {
                                // TODO: publish an IllegalDocumentState event to the application.
                                Logger?.LogError("IllegalDocumentState: " + triaged.ec);
                                insertMode = false;
                                cas = 0;
                                return RepeatAction.RepeatWithDelay;
                            }
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }).CAF();
        }

        private async Task SetAtrCommit()
        {
            _ = _atr ?? throw new InvalidOperationException($"{nameof(SetAtrCommit)} without initializing ATR.");

            await RepeatUntilSuccessOrThrow(async () =>
            {
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ATR_COMMIT);
                    await _testHooks.BeforeAtrCommit(this).CAF();
                    await _atr.MutateAtrCommit(_stagedMutations).CAF();
                    Logger?.LogDebug($"{nameof(SetAtrCommit)} for {Redactor.UserData(_atr.FullPath)} (attempt={AttemptId})");
                    await _testHooks.AfterAtrCommit(this).CAF();
                    _state = AttemptStates.COMMITTED;
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageSetAtrCommitErrors(ex);
                    if (triaged.ec == ErrorClass.FailExpiry)
                    {
                        _expirationOvertimeMode = true;
                    }
                    else if (triaged.ec == ErrorClass.FailAmbiguous)
                    {
                        return await RepeatUntilSuccessOrThrow<RepeatAction>(async () =>
                        {
                            var topRetry = await ResolveSetAtrCommitAmbiguity().CAF();
                            return (RepeatAction.NoRepeat, topRetry);
                        });
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }).CAF();
        }

        private async Task<RepeatAction> ResolveSetAtrCommitAmbiguity()
        {
            var setAtrCommitRetryAction = await RepeatUntilSuccessOrThrow<RepeatAction>(async () =>
            {
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION);
                    await _testHooks.BeforeAtrCommitAmbiguityResolution(this).CAF();
                    var refreshedStatus = await _atr!.LookupAtrState().CAF();
                    if (!Enum.TryParse<AttemptStates>(refreshedStatus, out var parsedRefreshStatus))
                    {
                        throw CreateError(this, ErrorClass.FailOther)
                            .Cause(new InvalidOperationException(
                                $"ATR state '{refreshedStatus}' could not be parsed"))
                            .DoNotRollbackAttempt()
                            .Build();
                    }

                    switch (parsedRefreshStatus)
                    {
                        case AttemptStates.COMMITTED:
                            // The ambiguous operation actually succeeded. Return success.
                            return (retry: RepeatAction.NoRepeat, finalVal: RepeatAction.NoRepeat);
                        case AttemptStates.PENDING:
                            // The ambiguous operation did not succeed. Restart from the top of SetATRCommit.
                            return (retry: RepeatAction.NoRepeat, RepeatAction.RepeatWithDelay);
                        case AttemptStates.ABORTED:
                        case AttemptStates.ROLLED_BACK:
                            // Another actor has aborted this transaction under us.
                            // Raise an Error(ec = FAIL_OTHER, rollback=false, cause=TransactionAbortedExternally)
                            throw CreateError(this, ErrorClass.FailOther)
                                .Cause(new TransactionAbortedExternallyException())
                                .DoNotRollbackAttempt()
                                .Build();
                        default:
                            // Unknown status, perhaps from a future protocol or extension.
                            // Bailout and leave the transaction for cleanup by raising
                            // Error(ec = FAIL_OTHER, rollback=false, cause=IllegalStateException
                            throw CreateError(this, ErrorClass.FailOther)
                                .Cause(new InvalidOperationException("Unknown state in ambiguity resolution."))
                                .DoNotRollbackAttempt()
                                .Build();
                    }
                }
                catch (Exception exAmbiguity)
                {
                    var triagedAmbiguity = _triage.TriageSetAtrCommitAmbiguityErrors(exAmbiguity);
                    switch (triagedAmbiguity.ec)
                    {
                        case ErrorClass.FailExpiry:
                            _expirationOvertimeMode = true;
                            goto default;
                        case ErrorClass.FailTransient:
                        case ErrorClass.FailOther:
                            // We can’t proceed until we’re resolved the ambiguity or expired, so retry from the top of this section, after waiting OpRetryDelay.
                            return (RepeatAction.RepeatWithDelay, RepeatAction.RepeatWithDelay);
                        default:
                            throw _triage.AssertNotNull(triagedAmbiguity, exAmbiguity);
                    }
                }
            });

            return setAtrCommitRetryAction;
        }

        private async Task SetAtrAborted(bool isAppRollback)
        {
            Logger?.LogInformation($"Setting Aborted status.  {nameof(isAppRollback)}={isAppRollback}");

            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#SetATRAborted
            await RepeatUntilSuccessOrThrow(async () =>
            {
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ATR_ABORT);
                    await _testHooks.BeforeAtrAborted(this).CAF();
                    await _atr!.MutateAtrAborted(_stagedMutations).CAF();
                    Logger?.LogDebug($"{nameof(SetAtrAborted)} for {Redactor.UserData(_atr.FullPath)} (attempt={AttemptId})");
                    await _testHooks.AfterAtrAborted(this).CAF();
                    _state = AttemptStates.ABORTED;
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    if (_expirationOvertimeMode)
                    {
                        throw CreateError(this, ErrorClass.FailExpiry)
                            .Cause(new AttemptExpiredException(this, "Expired in " + nameof(SetAtrAborted)))
                            .DoNotRollbackAttempt()
                            .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired)
                            .Build();
                    }

                    (ErrorClass ec, TransactionOperationFailedException? toThrow) = _triage.TriageSetAtrAbortedErrors(ex);
                    switch (ec)
                    {
                        case ErrorClass.FailExpiry:
                            _expirationOvertimeMode = true;
                            return RepeatAction.RepeatWithBackoff;
                        case ErrorClass.FailPathNotFound:
                        case ErrorClass.FailDocNotFound:
                        case ErrorClass.FailAtrFull:
                        case ErrorClass.FailHard:
                            throw toThrow ?? CreateError(this, ec, new InvalidOperationException("Failed to generate proper exception wrapper", ex))
                                .Build();

                        default:
                            return RepeatAction.RepeatWithBackoff;
                    }
                }
            });
        }

        private async Task SetAtrRolledBack()
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#SetATRRolledBack
            await RepeatUntilSuccessOrThrow(async () =>
            {
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ATR_ROLLBACK_COMPLETE);
                    await _testHooks.BeforeAtrRolledBack(this).CAF();
                    await _atr!.MutateAtrRolledBack().CAF();
                    Logger?.LogDebug($"{nameof(SetAtrRolledBack)} for {Redactor.UserData(_atr.FullPath)} (attempt={AttemptId})");
                    await _testHooks.AfterAtrRolledBack(this).CAF();
                    _state = AttemptStates.ROLLED_BACK;
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime(rollback: false);

                    (ErrorClass ec, TransactionOperationFailedException? toThrow) = _triage.TriageSetAtrRolledBackErrors(ex);
                    switch (ec)
                    {
                        case ErrorClass.FailPathNotFound:
                        case ErrorClass.FailDocNotFound:
                            // Whatever has happened, the necessary handling for all these is the same: continue as if success.
                            // The ATR entry has been removed
                            return RepeatAction.NoRepeat;
                        case ErrorClass.FailExpiry:
                        case ErrorClass.FailHard:
                            throw toThrow ?? CreateError(this, ec,
                                    new InvalidOperationException("Failed to generate proper exception wrapper", ex))
                                .Build();
                        default:
                            return RepeatAction.RepeatWithBackoff;
                    }
                }
            });
        }

        /// <summary>
        /// Rollback the transaction, explicitly.
        /// </summary>
        /// <returns>A task representing the asynchronous work.</returns>
        /// <remarks>Calling this method on AttemptContext is usually unnecessary, as unhandled exceptions will trigger a rollback automatically.</remarks>
        public Task RollbackAsync() => this.RollbackInternal(true);

        internal TransactionAttempt ToAttempt()
        {
            var atrInfo = _atr == null
                ? default
                : new DocRecord(_atr.BucketName, _atr.ScopeName, _atr.CollectionName,
                    _atr.AtrId);

            var ta = new TransactionAttempt()
            {
                AttemptId = AttemptId,
                AtrRecord = atrInfo,
                FinalState = _state,
                MutationTokens = _finalMutations.ToArray(),
                StagedInsertedIds = StagedInserts.Select(sm => sm.Doc.Id).ToArray(),
                StagedRemoveIds = StagedRemoves.Select(sm => sm.Doc.Id).ToArray(),
                StagedReplaceIds = StagedReplaces.Select(sm => sm.Doc.Id).ToArray()
            };

            return ta;
        }

        protected bool IsDone => _state != AttemptStates.NOTHING_WRITTEN && _state != AttemptStates.PENDING;

        internal async Task RollbackInternal(bool isAppRollback)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#rollbackInternal
            if (!_expirationOvertimeMode)
            {
                if (HasExpiredClientSide(null, hookPoint: ITestHooks.HOOK_ROLLBACK))
                {
                    _expirationOvertimeMode = true;
                }
            }

            if (_state == AttemptStates.NOTHING_WRITTEN)
            {
                return;
            }

            if (_state == AttemptStates.COMMITTED
            || _state == AttemptStates.COMPLETED
            || _state == AttemptStates.ROLLED_BACK)
            {
                throw ErrorBuilder.CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Cannot perform operations after the transaction has been comitted or rolled back."))
                    .DoNotRollbackAttempt()
                    .Build();
            }

            await SetAtrAborted(isAppRollback).CAF();
            foreach (var sm in _stagedMutations)
            {
                switch (sm.Type)
                {
                    case StagedMutationType.Insert:
                        await RollbackStagedInsert(sm).CAF();
                        break;
                    case StagedMutationType.Remove:
                    case StagedMutationType.Replace:
                        await RollbackStagedReplaceOrRemove(sm).CAF();
                        break;
                    default:
                        throw new InvalidOperationException(sm.Type + " is not a supported mutation type for rollback.");

                }
            }

            await SetAtrRolledBack().CAF();
        }

        private async Task RollbackStagedInsert(StagedMutation sm)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#RollbackAsync-Staged-InsertAsync
            await RepeatUntilSuccessOrThrow(async () =>
            {
                Logger.LogDebug($"[{AttemptId}] rolling back staged insert for {sm.Doc.FullyQualifiedId}");
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_DELETE_INSERTED, sm.Doc.Id);
                    await _testHooks.BeforeRollbackDeleteInserted(this, sm.Doc.Id).CAF();
                    await _docs.ClearTransactionMetadata(sm.Doc.Collection, sm.Doc.Id, sm.Doc.Cas, true).CAF();
                    Logger.LogDebug("Rolled back staged {type} for {id}", sm.Type, sm.Doc.Id);
                    await _testHooks.AfterRollbackDeleteInserted(this, sm.Doc.Id).CAF();
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime(rollback: false);

                    (ErrorClass ec, TransactionOperationFailedException? toThrow) = _triage.TriageRollbackStagedInsertErrors(ex);
                    switch (ec)
                    {
                        case ErrorClass.FailExpiry:
                            _expirationOvertimeMode = true;
                            return RepeatAction.RepeatWithBackoff;
                        case ErrorClass.FailDocNotFound:
                        case ErrorClass.FailPathNotFound:
                            // something must have succeeded in the interim after a retry
                            return RepeatAction.NoRepeat;
                        case ErrorClass.FailCasMismatch:
                        case ErrorClass.FailHard:
                            throw _triage.AssertNotNull(toThrow, ec, ex);
                        default:
                            return RepeatAction.RepeatWithBackoff;
                    }
                }
            }).CAF();
        }

        private async Task RollbackStagedReplaceOrRemove(StagedMutation sm)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#RollbackAsync-Staged-ReplaceAsync-or-RemoveAsync
            await RepeatUntilSuccessOrThrow(async () =>
            {
                Logger.LogDebug($"[{AttemptId}] rolling back staged replace or remove for {sm.Doc.FullyQualifiedId}");
                try
                {
                    ErrorIfExpiredAndNotInExpiryOvertimeMode(ITestHooks.HOOK_ROLLBACK_DOC, sm.Doc.Id);
                    await _testHooks.BeforeDocRolledBack(this, sm.Doc.Id).CAF();
                    await _docs.ClearTransactionMetadata(sm.Doc.Collection, sm.Doc.Id, sm.Doc.Cas, sm.Doc.IsDeleted);
                    Logger.LogDebug("Rolled back staged {type} for {id}", sm.Type, sm.Doc.Id);
                    await _testHooks.AfterRollbackReplaceOrRemove(this, sm.Doc.Id).CAF();
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime(rollback: false);

                    var tr = _triage.TriageRollbackStagedRemoveOrReplaceErrors(ex);
                    switch (tr.ec)
                    {
                        case ErrorClass.FailExpiry:
                            _expirationOvertimeMode = true;
                            return RepeatAction.RepeatWithBackoff;
                        case ErrorClass.FailPathNotFound:
                            // must have finished elsewhere.
                            return RepeatAction.NoRepeat;
                        case ErrorClass.FailDocNotFound:
                        case ErrorClass.FailCasMismatch:
                        case ErrorClass.FailHard:
                            throw _triage.AssertNotNull(tr, ex);
                        default:
                            return RepeatAction.RepeatWithBackoff;
                    }
                }
            }).CAF();
        }

        protected void DoneCheck()
        {
            if (IsDone)
            {
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Cannot perform operations after a transaction has been committed or rolled back."))
                    .DoNotRollbackAttempt()
                    .Build();
            }
        }

        protected void BailoutIfInOvertime(bool rollback, [CallerMemberName] string caller = nameof(BailoutIfInOvertime))
        {
            if (_expirationOvertimeMode)
            {
                var builder = CreateError(this, ErrorClass.FailExpiry)
                    .Cause(new AttemptExpiredException(this, "Expired in " + nameof(caller)))
                    .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired);
                if (!rollback)
                {
                    builder.DoNotRollbackAttempt();
                }

                throw builder.Build();
            }
        }

        protected async Task InitAtrIfNeeded(ICouchbaseCollection collection, string id)
        {
            var atrCollection = await collection.Scope.Bucket.DefaultCollectionAsync();
            var testHookAtrId = await _testHooks.AtrIdForVBucket(this, AtrIds.GetVBucketId(id));
            var atrId = AtrIds.GetAtrId(id);
            lock (_initAtrLock)
            {
                // TODO: AtrRepository should be built via factory to actually support mocking.
                _atr ??= new AtrRepository(
                    attemptId: AttemptId,
                    overallContext: _overallContext,
                    atrCollection: atrCollection,
                    atrId: atrId,
                    atrDurability: _config.DurabilityLevel,
                    testHookAtrId: testHookAtrId);
            }
        }

        protected void CheckExpiryAndThrow(string? docId, string hookPoint)
        {
            if (HasExpiredClientSide(docId, hookPoint))
            {
                _expirationOvertimeMode = true;
                throw CreateError(this, ErrorClass.FailExpiry)
                    .Cause(new AttemptExpiredException(this, $"Expired in '{hookPoint}'"))
                    .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired)
                    .Build();
            }
        }

        protected void ErrorIfExpiredAndNotInExpiryOvertimeMode(string hookPoint, string? docId = null, [CallerMemberName] string caller = "")
        {
            if (_expirationOvertimeMode)
            {
                Logger?.LogInformation($"[{AttemptId}] not doing expiry check in {hookPoint}/{caller} as already in expiry overtime mode.");
                return;
            }

            if (HasExpiredClientSide(docId, hookPoint))
            {
                Logger?.LogInformation($"[{AttemptId}] has expired in stage {hookPoint}/{caller}");
                throw new AttemptExpiredException(this, $"Attempt has expired in stage {hookPoint}/{caller}");
            }
        }

        internal bool HasExpiredClientSide(string? docId, [CallerMemberName] string hookPoint = "")
        {
            try
            {
                var over = _overallContext.IsExpired;
                var hook = _testHooks.HasExpiredClientSideHook(this, hookPoint, docId);
                if (over)
                {
                    Logger?.LogInformation($"expired in stage {hookPoint} /{AttemptId}");
                }

                if (hook)
                {
                    Logger?.LogInformation($"fake expiry in stage {hookPoint} / {AttemptId}");
                }

                return over || hook;
            }
            catch
            {
                Logger?.LogDebug("fake expiry due to throw in stage {hookPoint}", hookPoint);
                throw;
            }
        }

        internal async Task CheckWriteWriteConflict(TransactionGetResult gr, string interactionPoint)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#CheckWriteWriteConflict
            // This logic checks and handles a document X previously read inside a transaction, A, being involved in another transaction B.
            // It takes a TransactionGetResult gr variable.

            var sw = Stopwatch.StartNew();
            await RepeatUntilSuccessOrThrow(async () =>
            {
                Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)}@{interactionPoint} for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                await ForwardCompatibility.Check(this, interactionPoint, gr.TransactionXattrs?.ForwardCompatibility).CAF();
                var otherAtrFromDocMeta = gr.TransactionXattrs?.AtrRef;
                if (otherAtrFromDocMeta == null)
                {
                    Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)} no other txn for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    // If gr has no transaction Metadata, it’s fine to proceed.
                    return RepeatAction.NoRepeat;
                }

                if (gr.TransactionXattrs?.Id?.Transactionid == _overallContext.TransactionId)
                {
                    Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)} same txn for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    // Else, if transaction A == transaction B, it’s fine to proceed
                    return RepeatAction.NoRepeat;
                }

                // If the transaction has expired, enter ExpiryOvertimeMode and raise Error(ec=FAIL_EXPIRY, raise=TRANSACTION_EXPIRED).
                CheckExpiryAndThrow(gr.Id, ITestHooks.HOOK_CHECK_WRITE_WRITE_CONFLICT);

                // Do a lookupIn call to fetch the ATR entry for B.
                ICouchbaseCollection ? otherAtrCollection = null;
                try
                {
                    await _testHooks.BeforeCheckAtrEntryForBlockingDoc(this, _atr?.AtrId ?? string.Empty).CAF();

                    otherAtrCollection = _atr == null
                        ? await AtrRepository.GetAtrCollection(otherAtrFromDocMeta, gr.Collection).CAF()
                        : await _atr.GetAtrCollection(otherAtrFromDocMeta).CAF();
                }
                catch (Exception err)
                {
                    throw CreateError(this, ErrorClass.FailWriteWriteConflict, err)
                        .RetryTransaction()
                        .Build();
                }

                if (otherAtrCollection == null)
                {
                    // we couldn't get the ATR collection, which means that the entry was bad
                    // --OR-- the bucket/collection/scope was deleted/locked/rebalanced
                    // TODO: the spec gives no method of handling this.
                    throw CreateError(this, ErrorClass.FailHard)
                        .Cause(new Exception(
                            $"ATR entry '{Redactor.SystemData(gr?.TransactionXattrs?.AtrRef?.ToString())}' could not be read.",
                            new DocumentNotFoundException()))
                        .Build();
                }

                var txn = gr.TransactionXattrs ?? throw new ArgumentNullException(nameof(gr.TransactionXattrs));
                txn.ValidateMinimum();
                AtrEntry? otherAtr = _atr == null
                    ? await AtrRepository.FindEntryForTransaction(otherAtrCollection, txn.AtrRef!.Id!, txn.Id!.AttemptId!, _config.KeyValueTimeout).CAF()
                    : await _atr.FindEntryForTransaction(otherAtrCollection, txn.AtrRef!.Id!, txn.Id?.AttemptId).CAF();

                if (otherAtr == null)
                {
                    // cleanup occurred, OK to proceed.
                    Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)} cleanup occurred on other ATR for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    return RepeatAction.NoRepeat;
                }

                await ForwardCompatibility.Check(this, ForwardCompatibility.WriteWriteConflictReadingAtr, otherAtr.ForwardCompatibility).CAF();

                if (otherAtr.IsExpired == true)
                {
                    var expiredAt = (otherAtr.TimestampStartMsecs!.Value.AddMilliseconds(otherAtr.ExpiresAfterMsecs!.Value));
                    var utcNow = DateTimeOffset.UtcNow;
                    var expiredMsecs = (utcNow - expiredAt).TotalMilliseconds;
                    Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)} found expired (@{expiredAt}, i.e. {expiredMsecs}ms ago) other ATR for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    return RepeatAction.NoRepeat;
                }

                if (otherAtr.State == AttemptStates.COMPLETED || otherAtr.State == AttemptStates.ROLLED_BACK)
                {
                    // ok to proceed
                    Logger?.LogDebug($"{nameof(CheckWriteWriteConflict)} other ATR is {otherAtr.State} for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    return RepeatAction.NoRepeat;
                }

                if (sw.Elapsed > WriteWriteConflictTimeLimit)
                {
                    Logger?.LogWarning($"{nameof(CheckWriteWriteConflict)} CONFLICT DETECTED. Other ATR is {otherAtr.State} for {Redactor.UserData(gr.FullyQualifiedId)}, attempt={AttemptId}");
                    throw CreateError(this, ErrorClass.FailWriteWriteConflict)
                        .RetryTransaction()
                        .Build();
                }

                return RepeatAction.RepeatWithBackoff;
            }).CAF();
        }

        internal void SaveErrorWrapper(TransactionOperationFailedException ex)
        {
            _previousErrors.TryAdd(ex.ExceptionNumber, ex);
        }

        private enum RepeatAction
        {
            NoRepeat = 0,
            RepeatWithDelay = 1,
            RepeatNoDelay = 2,
            RepeatWithBackoff = 3
        }

        private async Task<ICouchbaseCollection> GetAtrCollection(AtrRef atrRef, ICouchbaseCollection anyCollection)
        {
            var getCollectionTask = _atr?.GetAtrCollection(atrRef)
                                    ?? AtrRepository.GetAtrCollection(atrRef, anyCollection);
            var docAtrCollection = await getCollectionTask.CAF()
                                   ?? throw new ActiveTransactionRecordNotFoundException();

            return docAtrCollection;
        }

        private async Task<T> RepeatUntilSuccessOrThrow<T>(Func<Task<(RepeatAction retry, T finalVal)>> func, int retryLimit = 100_000, [CallerMemberName] string caller = nameof(RepeatUntilSuccessOrThrow))
        {
            int retryCount = -1;
            int opRetryBackoffMs = 1;
            while (retryCount < retryLimit)
            {
                retryCount++;
                var result = await func().CAF();
                switch (result.retry)
                {
                    case RepeatAction.RepeatWithDelay:
                        await OpRetryDelay().CAF();
                        break;
                    case RepeatAction.RepeatWithBackoff:
                        await Task.Delay(opRetryBackoffMs).CAF();
                        opRetryBackoffMs = Math.Min(opRetryBackoffMs * 10, 100);
                        break;
                    case RepeatAction.RepeatNoDelay:
                        break;
                    default:
                        return result.finalVal;
                }
            }

            throw new InvalidOperationException($"Retry Limit ({retryLimit}) exceeded in method {caller}");
        }

        private Task RepeatUntilSuccessOrThrow(Func<Task<RepeatAction>> func, int retryLimit = 100_000, [CallerMemberName] string caller = nameof(RepeatUntilSuccessOrThrow)) =>
            RepeatUntilSuccessOrThrow<object>(async () =>
            {
                var retry = await func().CAF();
                return (retry, string.Empty);
            }, retryLimit, caller);

        private Task OpRetryDelay() => Task.Delay(Transactions.OpRetryDelay);

        internal CleanupRequest? GetCleanupRequest()
        {
            if (_atr == null
                || _state == AttemptStates.NOTHING_WRITTEN
                || _state == AttemptStates.COMPLETED
                || _state == AttemptStates.ROLLED_BACK)
            {
                // nothing to clean up
                Logger.LogInformation("Skipping addition of cleanup request in state {s}", _state);
                return null;
            }

            var cleanupRequest = new CleanupRequest(
                AttemptId: AttemptId,
                AtrId: _atr.AtrId,
                AtrCollection: _atr.Collection,
                InsertedIds: StagedInserts.Select(sm => sm.AsDocRecord()).ToList(),
                ReplacedIds: StagedReplaces.Select(sm => sm.AsDocRecord()).ToList(),
                RemovedIds: StagedRemoves.Select(sm => sm.AsDocRecord()).ToList(),
                State: _state,
                WhenReadyToBeProcessed: DateTimeOffset.UtcNow, // EXT_REMOVE_COMPLETED
                ProcessingErrors: new ConcurrentQueue<Exception>()
            );

            Logger.LogInformation("Adding collection for {col}/{atr} to run at {when}", cleanupRequest.AtrCollection.Name, cleanupRequest.AtrId, cleanupRequest.WhenReadyToBeProcessed);
            return cleanupRequest;
        }
    }
}


/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2021 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
