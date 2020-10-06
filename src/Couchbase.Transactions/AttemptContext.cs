﻿using System;
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
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.Attempts;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Error.Internal;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;
using DnsClient.Internal;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static Couchbase.Transactions.Error.ErrorBuilder;
using Exception = System.Exception;

namespace Couchbase.Transactions
{
    public class AttemptContext
    {
        private const int _sanityRetryLimit = 10000;
        private readonly TransactionContext _overallContext;
        private readonly TransactionConfig _config;
        private readonly Transactions _parent;
        private readonly ITestHooks _testHooks;
        internal IRedactor Redactor { get; }
        private readonly ITypeTranscoder _transcoder;
        private AttemptStates _state = AttemptStates.NOTHING_WRITTEN;
        private readonly ErrorTriage _triage;

        private readonly List<StagedMutation> _stagedMutations = new List<StagedMutation>();
        private readonly string _attemptId;

        private readonly object _initAtrLock = new object();
        private string? _atrId = null;
        private string? _atrBucketName = null;
        private string? _atrCollectionName = null;
        private string? _atrScopeName = null;
        private string? _atrLongCollectionName = null;
        private readonly DurabilityLevel _effectiveDurabilityLevel;
        private readonly List<MutationToken> _finalMutations = new List<MutationToken>();
        private ICouchbaseCollection? _atrCollection;
        private readonly ConcurrentDictionary<long, TransactionOperationFailedException> _previousErrors = new ConcurrentDictionary<long, TransactionOperationFailedException>();
        private bool _expirationOvertimeMode = false;

        internal AttemptContext(TransactionContext overallContext,
            TransactionConfig config,
            string attemptId,
            Transactions parent,
            ITestHooks? testHooks,
            IRedactor redactor,
            ITypeTranscoder transcoder,
            Microsoft.Extensions.Logging.ILoggerFactory? loggerFactory = null)
        {
            _attemptId = attemptId ?? throw new ArgumentNullException(nameof(attemptId));
            _overallContext = overallContext ?? throw new ArgumentNullException(nameof(overallContext));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            _testHooks = testHooks ?? DefaultTestHooks.Instance;
            Redactor = redactor ?? throw new ArgumentNullException(nameof(redactor));
            _transcoder = transcoder ?? throw new ArgumentNullException(nameof(transcoder));
            _effectiveDurabilityLevel = _overallContext.PerConfig?.DurabilityLevel ?? config.DurabilityLevel;
            Logger = loggerFactory?.CreateLogger<AttemptContext>();
            _triage = new ErrorTriage(this, _testHooks, loggerFactory);
        }

        public ILogger<AttemptContext>? Logger { get; }

        public async Task<TransactionGetResult?> GetOptionalAsync(ICouchbaseCollection collection, string id)
        {
            // TODO: Update this when adding non-throwing versions to NCBC itself so that GetOptionalAsync is the root and GetAsync calls it instead.
            try
            {
                return await GetAsync(collection, id).CAF();
            }
            catch (DocumentNotFoundException)
            {
                Logger?.LogInformation("Document '{id}' not found in collection '{collection.Name}'", Redactor.UserData(id), Redactor.UserData(collection));
                return null;
            }
        }

        public async Task<TransactionGetResult?> GetAsync(ICouchbaseCollection collection, string id)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiry();

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
                        return TransactionGetResult.FromOther(staged.Doc, staged.Content ?? Array.Empty<byte>(), TransactionJsonDocumentStatus.OwnWrite);
                    case StagedMutationType.Remove:
                        // LOGGER.info(attemptId, "found own-write of removed doc %s", RedactableArgument.redactUser(id));
                        return null;
                    default:
                        throw new InvalidOperationException($"Document '{Redactor.UserData(id)}' was staged with type {staged.Type}");
                }
            }

            // Do GetAsync a document with MAV logic section
            await _testHooks.BeforeDocGet(this, id).CAF();

            try
            {
                try
                {
                    // Do a Sub-Document lookup, getting all transactional metadata, the “$document” virtual xattr,
                    // and the document’s body. Timeout is set as in Timeouts.
                    //  TODO: Added for Protocol 2.0: set accessDeleted=true.
                    var docWithMeta = await LookupDocWithMetadata(collection, id).CAF();
                    var doc = docWithMeta.doc;
                    // check the transactional metadata to see if the doc is already involved in a transaction.
                    if (doc.Exists(docWithMeta.atrIdIndex))
                    {
                        // TODO: Implement "else if success, check the transactional metadata" section
                        // from https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#GetAsync-a-Document-With-MAV-Logic
                    }

                    TransactionGetResult getResult = TransactionGetResultFromLookupIn(collection, id, docWithMeta);

                    await _testHooks.AfterGetComplete(this, id).CAF();

                    return getResult;
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

        // TODO: move this to a repository class
        internal TransactionGetResult TransactionGetResultFromLookupIn(ICouchbaseCollection collection, string id,
            (ILookupInResult doc, int atrIdIndex, int transactionIdIndex, int attemptIdIndex, int stagedDataIndex, int
                atrBucketNameIndex, int atrColNameIndex, int transactionRestorePrefixOnlyIndex, int typeIndex, int
                metaDocumentIndex, int fullDocumentIndex) docWithMeta)
        {
            var doc = docWithMeta.doc;

            // TODO:  Not happy with this mess of logic spread between AttemptContext.cs and TransactionGetResult.FromLookupIn
            (string? casFromDocument, string? revIdFromDocument, ulong? expTimeFromDocument) = (null, null, null);
            if (doc.Exists(docWithMeta.metaDocumentIndex))
            {
                var docMeta = doc.ContentAs<JObject>(docWithMeta.metaDocumentIndex);
                casFromDocument = docMeta["CAS"].Value<string>();
                revIdFromDocument = docMeta["revid"].Value<string>();
                expTimeFromDocument = docMeta["exptime"].Value<ulong?>();
            }

            (string? casPreTxn, string? revIdPreTxn, ulong? expTimePreTxn) = (null, null, null);
            if (doc.Exists(docWithMeta.transactionRestorePrefixOnlyIndex))
            {
                var docMeta = doc.ContentAs<JObject>(docWithMeta.transactionRestorePrefixOnlyIndex);
                if (docMeta != null)
                {
                    casPreTxn = docMeta["CAS"].Value<string>();
                    revIdPreTxn = docMeta["revid"].Value<string>();
                    expTimePreTxn = docMeta["exptime"].Value<ulong?>();
                }
            }

            // HACK:  ContentAs<byte[]> is failing.
            ////var preTxnContent = doc.ContentAs<byte[]>(9);
            var asDynamic = doc.ContentAs<dynamic>(docWithMeta.fullDocumentIndex);
            var preTxnContent = GetContentBytes(asDynamic);

            TransactionGetResult getResult = TransactionGetResult.FromLookupIn(
                collection,
                id,
                TransactionJsonDocumentStatus.Normal,
                _transcoder,
                doc.Cas,
                preTxnContent,
                atrId: StringIfExists(doc, docWithMeta.atrIdIndex),
                transactionId: StringIfExists(doc, docWithMeta.transactionIdIndex),
                attemptId: StringIfExists(doc, docWithMeta.attemptIdIndex),
                stagedContent: StringIfExists(doc, docWithMeta.stagedDataIndex),
                atrBucketName: StringIfExists(doc, docWithMeta.atrBucketNameIndex),
                atrLongCollectionName: StringIfExists(doc, docWithMeta.atrColNameIndex),
                op: StringIfExists(doc, docWithMeta.typeIndex),
                casPreTxn: casPreTxn,
                revidPreTxn: revIdPreTxn,
                exptimePreTxn: expTimePreTxn,
                casFromDocument: casFromDocument,
                revidFromDocument: revIdFromDocument,
                exptimeFromDocument: expTimeFromDocument
            );
            return getResult;
        }

        // TODO: move this to a repository class
        internal async Task<(
            ILookupInResult doc,
            int atrIdIndex,
            int transactionIdIndex,
            int attemptIdIndex,
            int stagedDataIndex,
            int atrBucketNameIndex,
            int atrColNameIndex,
            int transactionRestorePrefixOnlyIndex,
            int typeIndex,
            int metaDocumentIndex,
            int fullDocumentIndex)> LookupDocWithMetadata(ICouchbaseCollection collection, string id)
        {
            // TODO: promote this to a real class/struct, rather than just a named tuple.
            var doc = await collection.LookupInAsync(
                id,
                specs =>
                    specs.Get(TransactionFields.AtrId, true) // 0
                        .Get(TransactionFields.TransactionId, isXattr: true) // 1
                        .Get(TransactionFields.AttemptId, isXattr: true) // 2
                        .Get(TransactionFields.StagedData, isXattr: true) // 3
                        .Get(TransactionFields.AtrBucketName, isXattr: true) // 4
                        .Get(TransactionFields.AtrCollName, isXattr: true) // 5
                        .Get(TransactionFields.TransactionRestorePrefixOnly, true) // 6
                        .Get(TransactionFields.Type, true) // 7
                        .Get("$document", true) //  8
                        .GetFull(), // 9
                opts => opts.Timeout(_config.KeyValueTimeout)
            ).CAF();

            return (doc, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
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

        private string? StringIfExists(ILookupInResult doc, int index) => doc.Exists(index) ? doc.ContentAs<string>(index) : null;

        private StagedMutation FindStaged(ICouchbaseCollection collection, string id)
        {
            return _stagedMutations.Find(sm => sm.Doc.Id == id
                                               && sm.Doc.Collection.Name == collection.Name
                                               && sm.Doc.Collection.Scope.Name == collection.Scope.Name
                                               && sm.Doc.Collection.Scope.Bucket.Name == collection.Scope.Bucket.Name);
        }

        private StagedMutation FindStaged(TransactionGetResult doc) => FindStaged(doc.Collection, doc.Id);

        public async Task<TransactionGetResult> ReplaceAsync(TransactionGetResult doc, object content)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiry();
            await CheckWriteWriteConflict(doc).CAF();
            InitAtrIfNeeded(doc.Collection, doc.Id);
            await SetAtrPendingIfFirstMutation(doc.Collection);

            // TODO: re-evaluate accessDeleted after CreateAsDeleted is implemented.
            return await CreateStagedReplace(doc, content, accessDeleted: false);
        }

        private async Task SetAtrPendingIfFirstMutation(ICouchbaseCollection collection)
        {
            if (_stagedMutations.Count == 0)
            {
                await SetAtrPending(collection);
            }
        }

        private async Task<TransactionGetResult> CreateStagedReplace(TransactionGetResult doc, object content, bool accessDeleted)
        {
            try
            {
                try
                {
                    await _testHooks.BeforeStagedReplace(this, doc.Id);
                    var contentBytes = GetContentBytes(content);
                    var specs = CreateMutationOps("replace");
                    specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData, contentBytes, isXattr: true));

                    if (doc.DocumentMetadata != null)
                    {
                        var dm = doc.DocumentMetadata;
                        if (dm.Cas != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnCas, dm.Cas, isXattr: true));
                        }

                        if (dm.RevId != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnRevid, dm.RevId, isXattr: true));
                        }

                        if (dm.ExpTime != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnExptime, dm.ExpTime, isXattr: true));
                        }
                    }

                    var opts = new MutateInOptions()
                        .Cas(doc.Cas)
                        .StoreSemantics(StoreSemantics.Replace)
                        .Durability(_effectiveDurabilityLevel);

                    var updatedDoc = await doc.Collection.MutateInAsync(doc.Id, specs, opts).CAF();
                    await _testHooks.AfterStagedReplaceComplete(this, doc.Id).CAF();

                    doc.Cas = updatedDoc.Cas;

                    var stagedOld = FindStaged(doc);
                    if (stagedOld != null)
                    {
                        _stagedMutations.Remove(stagedOld);
                    }

                    if (stagedOld?.Type == StagedMutationType.Insert)
                    {
                        // If doc is already in stagedMutations as an INSERT or INSERT_SHADOW, then remove that, and add this op as a new INSERT or INSERT_SHADOW(depending on what was replaced).
                        _stagedMutations.Add(new StagedMutation(doc, contentBytes, StagedMutationType.Insert, updatedDoc));
                    }
                    else
                    {
                        // If doc is already in stagedMutations as a REPLACE, then overwrite it.
                        _stagedMutations.Add(new StagedMutation(doc, contentBytes, StagedMutationType.Replace, updatedDoc));
                    }

                    return TransactionGetResult.FromInsert(
                        doc.Collection,
                        doc.Id,
                        contentBytes,
                        _overallContext.TransactionId,
                        _attemptId,
                        _atrId!,
                        _atrBucketName!,
                        _atrScopeName!,
                        _atrCollectionName!,
                        updatedDoc,
                        _transcoder);
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

        private List<MutateInSpec> CreateMutationOps(string op)
        {
            var specs = new List<MutateInSpec>
            {
                MutateInSpec.Upsert(TransactionFields.TransactionId, _overallContext.TransactionId,
                    createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AttemptId, _attemptId, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrId, _atrId, createPath: true, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrBucketName, _atrBucketName, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.AtrCollName, _atrLongCollectionName, isXattr: true),
                MutateInSpec.Upsert(TransactionFields.Type, op, createPath: true, isXattr: true),
            };

            return specs;
        }

        public async Task<TransactionGetResult> InsertAsync(ICouchbaseCollection collection, string id, object content)
        {
            // TODO:  How does the user specify expiration?
            DoneCheck();
            CheckExpiry();
            InitAtrIfNeeded(collection, id);
            await SetAtrPendingIfFirstMutation(collection);

            // If this document already exists in StagedMutation, raise Error(FAIL_OTHER, cause=IllegalStateException [or platform-specific equivalent]).
            if (_stagedMutations.Any(sm => sm.Doc.FullyQualifiedId == TransactionGetResult.GetFullyQualifiedId(collection, id)))
            {
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Document is already staged for a mutation."))
                    .Build();
            }

            return await CreateStagedInsert(collection, id, content).CAF();
        }

        private async Task<TransactionGetResult> CreateStagedInsert(ICouchbaseCollection collection, string id, object content, ulong? cas = null)
        {
            try
            {
                var result = await RepeatUntilSuccessOrThrow<TransactionGetResult?>(async () =>
                {
                    try
                    {
                        // Check expiration again, since insert might be retried.
                        CheckExpiry();

                        await _testHooks.BeforeStagedInsert(this, id).CAF();

                        var contentBytes = GetContentBytes(content);
                        List<MutateInSpec> specs = CreateMutationOps("insert");
                        specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData, contentBytes, isXattr: true));

                        var mutateResult = await collection.MutateInAsync(id, specs, opts =>
                        {
                            var mutateCas = cas;
                            if (mutateCas.HasValue)
                            {
                                opts.Cas(mutateCas.Value).StoreSemantics(StoreSemantics.Replace);
                            }
                            else
                            {
                                opts.StoreSemantics(StoreSemantics.Insert);
                            }

                            opts.Durability(_effectiveDurabilityLevel);

                            // TODO: accessDeleted = true (needs NCBC-2573)
                            // TODO: createAsDeleted = true (needs NCBC-2573)
                        }).CAF();

                        var getResult = TransactionGetResult.FromInsert(
                            collection,
                            id,
                            contentBytes,
                            _overallContext.TransactionId,
                            _attemptId,
                            _atrId!,
                            _atrBucketName!,
                            _atrScopeName!,
                            _atrCollectionName!,
                            mutateResult,
                            _transcoder);

                        await _testHooks.AfterStagedInsertComplete(this, id).CAF();

                        var stagedMutation = new StagedMutation(getResult, contentBytes, StagedMutationType.Insert,
                            mutateResult);
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
                                await RepeatUntilSuccessOrThrow(async () =>
                                {
                                    // handle FAIL_DOC_ALREADY_EXISTS
                                    try
                                    {
                                        await _testHooks.BeforeGetDocInExistsDuringStagedInsert(this, id).CAF();
                                        var docWithMeta = await LookupDocWithMetadata(collection, id).CAF();
                                        var doc = docWithMeta.doc;

                                        var docInATransaction =
                                            doc.Exists(docWithMeta.atrIdIndex) &&
                                            !string.IsNullOrEmpty(doc.ContentAs<string>(docWithMeta.atrIdIndex));

                                        // TODO: handle tombstone detection when NCBC-2573 is done.
                                        var docIsTombstone = false;
                                        if (docIsTombstone && !docInATransaction)
                                        {
                                            // If the doc is a tombstone and not in any transaction
                                            // -> It’s ok to go ahead and overwrite.
                                            // Perform this algorithm from the top with cas=the cas from the get.
                                            cas = doc.Cas;
                                            return RepeatAction.RepeatNoDelay;
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
                                            var getResult = TransactionGetResultFromLookupIn(collection, id, docWithMeta);
                                            await CheckWriteWriteConflict(getResult).CAF();

                                            // If this logic succeeds, we are ok to overwrite the doc.
                                            // Perform this algorithm from the top, with cas=the cas from the get.
                                            cas = getResult.Cas;
                                            return RepeatAction.RepeatNoDelay;
                                        }
                                    }
                                    catch (Exception exDocExists)
                                    {
                                        var triagedDocExists = _triage.TriageDocExistsOnStagedInsertErrors(ex);
                                        throw _triage.AssertNotNull(triagedDocExists, exDocExists);
                                    }
                                }).CAF();

                                return (RepeatAction.NoRepeat, null);
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


        private async Task SetAtrPending(ICouchbaseCollection collection)
        {
            _atrId = _atrId ?? throw new InvalidOperationException("atrId is not present");

            try
            {
                await RepeatUntilSuccessOrThrow(async () =>
                {
                    try
                    {
                        CheckExpiry();
                        await _testHooks.BeforeAtrPending(this);

                        var prefix = $"attempts.{_attemptId}";
                        var t1 = _overallContext.StartTime;
                        var t2 = DateTimeOffset.UtcNow;
                        var tElapsed = t2 - t1;
                        var tc = _config.ExpirationTime;
                        var tRemaining = tc - tElapsed;
                        var exp = (ulong)Math.Max(Math.Min(tRemaining.TotalMilliseconds, tc.TotalMilliseconds), 0);

                        var mutateInResult = await collection.MutateInAsync(_atrId, specs =>
                                specs.Insert($"{prefix}.{TransactionFields.AtrFieldTransactionId}",
                                        _overallContext.TransactionId,
                                        createPath: true, isXattr: true)
                                    .Insert($"{prefix}.{TransactionFields.AtrFieldStatus}",
                                        AttemptStates.PENDING.ToString(), createPath: false, isXattr: true)
                                    .Insert($"{prefix}.{TransactionFields.AtrFieldStartTimestamp}", MutationMacro.Cas)
                                    .Insert($"{prefix}.{TransactionFields.AtrFieldExpiresAfterMsecs}", exp,
                                        createPath: false, isXattr: true),
                            opts => opts.StoreSemantics(StoreSemantics.Upsert)
                        ).CAF();

                        var lookupInResult = await collection.LookupInAsync(_atrId,
                            specs => specs.Get($"{prefix}.{TransactionFields.AtrFieldStartTimestamp}", isXattr: true));
                        var fetchedCas = lookupInResult.ContentAs<string>(0);
                        var getResult = await collection.GetAsync(_atrId).CAF();
                        var atr = getResult.ContentAs<dynamic>();
                        await _testHooks.AfterAtrPending(this);
                        _state = AttemptStates.PENDING;
                        return RepeatAction.NoRepeat;
                    }
                    catch (Exception ex)
                    {
                        var triaged = _triage.TriageSetAtrPendingErrors(ex, _expirationOvertimeMode);
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

        private byte[] GetContentBytes(object content)
        {
            using var memoryStream = new MemoryStream();
            var flags = _transcoder.GetFormat(content);
            _transcoder.Encode(memoryStream, content, flags, OpCode.Add);
            var contentBytes = memoryStream.GetBuffer();
            return contentBytes;
        }

        public async Task RemoveAsync(TransactionGetResult doc)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiry();
            if (StagedInserts.Any(sm => sm.Doc.FullyQualifiedId == doc.FullyQualifiedId))
            {
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Document is already staged for insert."))
                    .Build();
            }

            await CheckWriteWriteConflict(doc).CAF();
            InitAtrIfNeeded(doc.Collection, doc.Id);
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
                    var specs = CreateMutationOps(op: "remove");
                    specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData,
                        TransactionFields.StagedDataRemoveKeyword, isXattr: true));

                    if (doc.DocumentMetadata != null)
                    {
                        var dm = doc.DocumentMetadata;
                        if (dm.Cas != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnCas, dm.Cas, isXattr: true));
                        }

                        if (dm.RevId != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnRevid, dm.RevId, isXattr: true));
                        }

                        if (dm.ExpTime != null)
                        {
                            specs.Add(MutateInSpec.Upsert(TransactionFields.PreTxnExptime, dm.ExpTime, isXattr: true));
                        }
                    }

                    var opts = new MutateInOptions()
                        .Cas(doc.Cas)
                        .StoreSemantics(StoreSemantics.Replace)
                        .Durability(_effectiveDurabilityLevel);

                    // TODO: set accessDeleted after NCBC-2573 is done.
                    var updatedDoc = await doc.Collection.MutateInAsync(doc.Id, specs, opts).CAF();
                    await _testHooks.AfterStagedRemoveComplete(this, doc.Id).CAF();

                    doc.Cas = updatedDoc.Cas;
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

                    var stagedRemove = new StagedMutation(doc, ActiveTransactionRecord.RemovePlaceholderBytes,
                        StagedMutationType.Remove, updatedDoc);
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
            CheckExpiry();
            DoneCheck();
            lock (_initAtrLock)
            {
                if (_atrId == null)
                {
                    // If no ATR has been selected then no mutation has been performed. Return success.
                    // This will leave state as NOTHING_WRITTEN,
                    return;
                }
            }

            await SetAtrCommit().CAF();
            await UnstageDocs().CAF();
            await SetAtrComplete().CAF();
        }

        private async Task SetAtrComplete()
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#SetATRComplete
            try
            {
                await _testHooks.BeforeAtrComplete(this).CAF();
                var prefix = $"attempts.{_attemptId}";
                var specs = new MutateInSpec[]
                {
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}", AttemptStates.COMPLETED.ToString(), isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldTimestampComplete}", MutationMacro.Cas)
                };

                var mutateResult = await _atrCollection!
                    .MutateInAsync(_atrId!, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();

                await _testHooks.AfterAtrComplete(this).CAF();
                _state = AttemptStates.COMPLETED;
            }
            catch (Exception ex)
            {
                var triaged = _triage.TriageSetAtrCompleteErrors(ex);
                if (triaged.toThrow != null)
                {
                    throw triaged.toThrow;
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
                    CheckExpiry();
                    var opts = new RemoveOptions().Cas(0).Durability(_effectiveDurabilityLevel);
                    if (_overallContext.Config.KeyValueTimeout.HasValue)
                    {
                        opts.Timeout(_overallContext.Config.KeyValueTimeout.Value);
                    }

                    await sm.Doc.Collection.RemoveAsync(sm.Doc.Id, opts).CAF();

                    Logger?.LogDebug(
                        $"Unstaged RemoveAsync successfully for {Redactor.UserData(sm.Doc.FullyQualifiedId)} (retryCount={retryCount}");
                    await _testHooks.AfterDocRemovedPreRetry(this, sm.Doc.Id).CAF();

                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageUnstageRemoveErrors(ex, _expirationOvertimeMode);
                    switch (triaged.ec)
                    {
                        case ErrorClass.FailAmbiguous:
                            ambiguityResolutionMode = true;
                            return RepeatAction.RepeatWithDelay;
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }).CAF();

            _finalMutations.Add(sm.MutationResult.MutationToken);
            await _testHooks.AfterDocRemovedPostRetry(this, sm.Doc.Id).CAF();
        }

        private Task<(ulong cas, byte[] content)> FetchIfNeededBeforeUnstage(StagedMutation sm)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#FetchIfNeededBeforeUnstage
            // TODO: consider implementing ExtMemoryOptUnstaging mode
            // For now, assuming ExtTimeOptUnstaging mode...
            return Task.FromResult((sm.Doc.Cas, sm.Content));
        }

        private async Task UnstageInsertOrReplace(StagedMutation sm, ulong cas, byte[] content, bool insertMode = false, bool ambiguityResolutionMode = false)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Inserts-and-Replaces-Protocol-20-version
            await _testHooks.BeforeDocCommitted(this, sm.Doc.Id).CAF();

            await RepeatUntilSuccessOrThrow(async () =>
            {
                CheckExpiry();
                try
                {
                    IMutationResult mutateResult;
                    var finalDoc = _transcoder.Serializer.Deserialize<JObject>(content);
                    if (insertMode)
                    {
                        // TODO:  InsertAsync or  upsert?
                        // Try again with InsertAsync after CreateAsDeleted is implemented.
                        mutateResult = await sm.Doc.Collection.UpsertAsync(sm.Doc.Id, finalDoc).CAF();
                    }
                    else
                    {
                        mutateResult = await sm.Doc.Collection.MutateInAsync(sm.Doc.Id, specs =>
                                    specs
                                        // NCBC-2639
                                        ////.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null,
                                        ////    isXattr: true)
                                        .Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                                        .SetDoc(finalDoc),
                                opts => opts.Cas(cas).StoreSemantics(StoreSemantics.Replace))
                            .CAF();
                    }

                    Logger?.LogInformation(
                        $"Unstaged mutation successfully on {Redactor.UserData(sm.Doc.FullyQualifiedId)} with insertMode={insertMode}, ambiguityResolutionMode={ambiguityResolutionMode}");

                    if (mutateResult?.MutationToken != null)
                    {
                        _finalMutations.Add(mutateResult.MutationToken);
                    }

                    await _testHooks.AfterDocCommittedBeforeSavingCas(this, sm.Doc.Id);

                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageUnstageInsertOrReplaceErrors(ex, _expirationOvertimeMode);
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
            _ = _atrCollection ??
                throw new InvalidOperationException($"{nameof(SetAtrCommit)} without initializing ATR.");
            var prefix = $"attempts.{_attemptId}";
            await RepeatUntilSuccessOrThrow(async () =>
            {
                try
                {
                    CheckExpiry();
                    await _testHooks.BeforeAtrCommit(this).CAF();

                    var inserts = new JArray(StagedInserts.Select(sm => sm.ForAtr()));
                    var replaces = new JArray(StagedReplaces.Select(sm => sm.ForAtr()));
                    var removes = new JArray(StagedRemoves.Select(sm => sm.ForAtr()));
                    var specs = new MutateInSpec[]
                    {
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}",
                            AttemptStates.COMMITTED.ToString(), isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStartCommit}", MutationMacro.Cas),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsInserted}", inserts,
                            isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsReplaced}", replaces,
                            isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsRemoved}", removes,
                            isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldPendingSentinel}", 0,
                            isXattr: true)
                    };


                    var mutateInResult = await _atrCollection
                        .MutateInAsync(_atrId!, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();

                    await _testHooks.AfterAtrCommit(this).CAF();
                    _state = AttemptStates.COMMITTED;
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    var triaged = _triage.TriageSetAtrCommitErrors(ex);
                    if (triaged.ec == ErrorClass.FailAmbiguous)
                    {
                        return await RepeatUntilSuccessOrThrow<RepeatAction>(async () =>
                        {
                            var topRetry = await ResolveSetAtrCommitAmbiguity(prefix).CAF();
                            return (RepeatAction.NoRepeat, topRetry);
                        });
                    }

                    throw _triage.AssertNotNull(triaged, ex);
                }
            }).CAF();
        }

        private async Task<RepeatAction> ResolveSetAtrCommitAmbiguity(string prefix)
        {
            var setAtrCommitRetryAction = await RepeatUntilSuccessOrThrow<RepeatAction>(async () =>
            {
                try
                {
                    CheckExpiry();
                    await _testHooks.BeforeAtrCommitAmiguityResolution(this).CAF();
                    var lookupInResult = await _atrCollection!.LookupInAsync(_atrId!,
                            specs => specs.Get($"{prefix}.{TransactionFields.AtrFieldStatus}", isXattr: true))
                        .CAF();
                    var refreshedStatus = lookupInResult.ContentAs<string>(0);
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
                    CheckExpiry();

                    await _testHooks.BeforeAtrAborted(this).CAF();
                    var prefix = $"attempts.{_attemptId}";

                    var inserts = new JArray(StagedInserts.Select(sm => sm.ForAtr()));
                    var replaces = new JArray(StagedReplaces.Select(sm => sm.ForAtr()));
                    var removes = new JArray(StagedRemoves.Select(sm => sm.ForAtr()));
                    var specs = new MutateInSpec[]
                    {
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}",
                            AttemptStates.ABORTED.ToString(), isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldTimestampRollbackStart}",
                            MutationMacro.Cas),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsInserted}", inserts,
                            isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsReplaced}", replaces,
                            isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsRemoved}", removes,
                            isXattr: true),
                    };

                    var mutateInResult = await _atrCollection!
                        .MutateInAsync(_atrId!, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();


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
                    CheckExpiry();
                    await _testHooks.BeforeAtrRolledBack(this).CAF();
                    var prefix = $"attempts.{_attemptId}";

                    var specs = new MutateInSpec[]
                    {
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}",
                            AttemptStates.ROLLED_BACK.ToString(), isXattr: true),
                        MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldTimestampRollbackComplete}",
                            MutationMacro.Cas),
                    };

                    _ = await _atrCollection!
                        .MutateInAsync(_atrId!, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();

                    await _testHooks.AfterAtrRolledBack(this).CAF();
                    _state = AttemptStates.ROLLED_BACK;
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime();

                    (ErrorClass ec, TransactionOperationFailedException? toThrow) = _triage.TriageSetAtrRolledBackErrors(ex);
                    switch (ec)
                    {
                        case ErrorClass.FailExpiry:
                            return RepeatAction.RepeatWithBackoff;
                        case ErrorClass.FailPathNotFound:
                            // perhaps the cleanup process has removed it? Success!?
                            return RepeatAction.NoRepeat;
                        case ErrorClass.FailDocNotFound:
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

        public Task RollbackAsync() => this.RollbackInternal(true);

        public Task DeferAsync() => throw new NotImplementedException();

        internal TransactionAttempt ToAttempt()
        {
            var ta = new TransactionAttempt()
            {
                AttemptId = _attemptId,
                AtrCollection = _atrCollection,
                AtrId = _atrId,
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
            CheckExpiry();
            if (_state == AttemptStates.NOTHING_WRITTEN)
            {
                return;
            }

            if (_state == AttemptStates.COMMITTED
            || _state == AttemptStates.COMPLETED
            || _state == AttemptStates.ROLLED_BACK)
            {
                // TODO: Check ErrorClass vs. Java impl.
                throw ErrorBuilder.CreateError(this, ErrorClass.FailOther).DoNotRollbackAttempt().Build();
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

                try
                {
                    CheckExpiry();
                    await _testHooks.BeforeRollbackDeleteInserted(this, sm.Doc.Id).CAF();
                    var opts = new MutateInOptions()
                        .Cas(sm.Doc.Cas)
                        .Durability(_effectiveDurabilityLevel);
                    if (_config.KeyValueTimeout.HasValue)
                    {
                        opts.Timeout(_config.KeyValueTimeout.Value);
                    }

                    var specs = new MutateInSpec[]
                    {
                        // NCBC-2639
                        ////.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null,
                        ////    isXattr: true)
                        MutateInSpec.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                    };


                    var mutateInResult = sm.Doc.Collection.MutateInAsync(sm.Doc.Id,
                        specs, opts).CAF();

                    await _testHooks.AfterRollbackDeleteInserted(this, sm.Doc.Id).CAF();
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime();

                    (ErrorClass ec, TransactionOperationFailedException? toThrow) = _triage.TriageRollbackStagedInsertErrors(ex);
                    switch (ec)
                    {
                        case ErrorClass.FailExpiry:
                            return RepeatAction.RepeatWithBackoff;
                        case ErrorClass.FailDocNotFound:
                        case ErrorClass.FailPathNotFound:
                            // something must have succeeded in the interim after a retry
                            return RepeatAction.NoRepeat;
                        case ErrorClass.FailCasMismatch:
                        case ErrorClass.FailHard:
                            throw toThrow ?? CreateError(this, ec,
                                    new InvalidOperationException("Failed to generate proper exception wrapper", ex))
                                .Build();
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
                try
                {
                    CheckExpiry();
                    await _testHooks.BeforeDocRolledBack(this, sm.Doc.Id).CAF();

                    // TODO: this appears identical to RollbackStagedInsert. Refactor to repository.
                    var opts = new MutateInOptions()
                        .Cas(sm.Doc.Cas)
                        .Durability(_effectiveDurabilityLevel);
                    if (_config.KeyValueTimeout.HasValue)
                    {
                        opts.Timeout(_config.KeyValueTimeout.Value);
                    }

                    var specs = new MutateInSpec[]
                    {
                        // NCBC-2639
                        ////.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null,
                        ////    isXattr: true)
                        MutateInSpec.Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)
                    };
                    var mutateInResult = sm.Doc.Collection.MutateInAsync(sm.Doc.Id, specs, opts).CAF();
                    await _testHooks.AfterRollbackReplaceOrRemove(this, sm.Doc.Id).CAF();
                    return RepeatAction.NoRepeat;
                }
                catch (Exception ex)
                {
                    BailoutIfInOvertime();

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

        protected void BailoutIfInOvertime([CallerMemberName] string caller = nameof(BailoutIfInOvertime))
        {
            if (_expirationOvertimeMode)
            {
                throw CreateError(this, ErrorClass.FailExpiry)
                    .Cause(new AttemptExpiredException(this, "Expired in " + nameof(caller)))
                    .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired)
                    .Build();
            }
        }

        protected void InitAtrIfNeeded(ICouchbaseCollection collection, string id)
        {
            lock (_initAtrLock)
            {
                if (_atrId == null)
                {
                    _atrId = AtrIds.GetAtrId(id);
                    _atrCollection = collection;
                    _atrCollectionName = collection.Name;
                    _atrScopeName = collection.Scope.Name;
                    _atrBucketName = collection.Scope.Bucket.Name;
                    _atrLongCollectionName = _atrScopeName + "." + _atrCollectionName;

                    // TODO:  check consistency on use of _atrCollectionName vs. _atrLongCollectionName
                    ////LOGGER.info(attemptId, "First mutated doc in txn is '%s' on vbucket %d, so using atr %s",
                    ////    RedactableArgument.redactUser(id), vbucketIdForDoc, atr);
                }
            }
        }

        protected void CheckExpiry([CallerMemberName] string caller = nameof(AttemptContext))
        {
            if (_overallContext.IsExpired)
            {
                _expirationOvertimeMode = true;
                throw CreateError(this, ErrorClass.FailExpiry)
                    .Cause(new AttemptExpiredException(this, $"Expired in {caller}"))
                    .RaiseException(TransactionOperationFailedException.FinalError.TransactionExpired)
                    .Build();
            }
        }

        // TODO: move this to a repository class
        internal async Task CheckWriteWriteConflict(TransactionGetResult gr)
        {
            //This logic checks and handles a document X previously read inside a transaction, A, being involved in another transaction B. It takes a TransactionGetResult gr variable.
            if (gr.Links?.AtrBucketName == null)
            {
                // If gr has no transaction Metadata, it’s fine to proceed.
                return;
            }

            if (gr.Links.StagedTransactionId == _overallContext.TransactionId)
            {
                // Else, if transaction A == transaction B, it’s fine to proceed
                return;
            }

            // If the transaction has expired, enter ExpiryOvertimeMode and raise Error(ec=FAIL_EXPIRY, raise=TRANSACTION_EXPIRED).
            CheckExpiry();

            await _testHooks.BeforeCheckAtrEntryForBlockingDoc(this, gr.Id).CAF();

            // Do a lookupIn call to fetch the ATR entry for B.
            // TODO:  Finish implementing
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#CheckWriteWriteConflict
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

        private enum StagedMutationType
        {
            Undefined = 0,
            Insert = 1,
            Remove = 2,
            Replace = 3
        }

        private class StagedMutation
        {
            public TransactionGetResult Doc { get; }
            public byte[] Content { get; }
            public StagedMutationType Type { get; }
            public IMutationResult MutationResult { get; }

            public StagedMutation(TransactionGetResult doc, byte[] content, StagedMutationType type, IMutationResult mutationResult)
            {
                Doc = doc;
                Content = content;
                Type = type;
                MutationResult = mutationResult;
            }

            public JObject ForAtr() => new JObject(
                    new JProperty(TransactionFields.AtrFieldPerDocId, Doc.Id),
                    new JProperty(TransactionFields.AtrFieldPerDocBucket, Doc.Collection.Scope.Bucket.Name),
                    new JProperty(TransactionFields.AtrFieldPerDocScope, Doc.Collection.Scope.Name),
                    new JProperty(TransactionFields.AtrFieldPerDocCollection, Doc.Collection.Name)
                );

        }
    }
}
