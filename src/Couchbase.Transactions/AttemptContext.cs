using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
using Couchbase.Transactions.Error.external;
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
        private readonly ConcurrentDictionary<long, ErrorWrapperException> _previousErrors = new ConcurrentDictionary<long, ErrorWrapperException>();
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
        }

        public ILogger<AttemptContext>? Logger { get; }

        public async Task<TransactionGetResult?> GetOptional(ICouchbaseCollection collection, string id)
        {
            // TODO: Update this when adding non-throwing versions to NCBC itself so that GetOptional is the root and Get calls it instead.
            try
            {
                return await Get(collection, id).CAF();
            }
            catch (DocumentNotFoundException)
            {
                Logger?.LogInformation("Document '{id}' not found in collection '{collection.Name}'", Redactor.UserData(id), Redactor.UserData(collection));
                return null;
            }
        }

        public async Task<TransactionGetResult?> Get(ICouchbaseCollection collection, string id)
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

            // Do Get a document with MAV logic section
            await _testHooks.BeforeDocGet(this, id).CAF();

            try
            {
                try
                {
                    // Do a Sub-Document lookup, getting all transactional metadata, the “$document” virtual xattr,
                    // and the document’s body. Timeout is set as in Timeouts.
                    //  TODO: Added for Protocol 2.0: set accessDeleted=true.
                    var docWithMeta = await LookupDocWithMetada(collection, id).CAF();
                    var doc = docWithMeta.doc;
                    // check the transactional metadata to see if the doc is already involved in a transaction.
                    if (doc.Exists(docWithMeta.atrIdIndex))
                    {
                        // TODO: updated spec
                        /*
                         * if resolvingMissingATREntry == the attemptId in the transactional metadata:
                           This is our second attempt getting the document, and it’s in the same state as
                           before (see ActiveTransactionRecordEntryNotFound discussion below). The blocking
                           transaction must have been cleaned up in PENDING state, leaving this document with
                           metadata. We are fine to return the body of the document to the app. Except if it
                           is a staged insert (e.g. it’s a tombstone - protocol 2 - or has a null body -
                           protocol 1), in which case return empty.
                         */
                    }

                    TransactionGetResult getResult = TransactionGetResultFromLookupIn(collection, id, docWithMeta);

                    await _testHooks.AfterGetComplete(this, id).CAF();

                    return getResult;
                }
                catch (Exception e)
                {
                    /*
                     * On error err of any of the above, classify as ErrorClass ec then:
                           FAIL_DOC_NOT_FOUND -> return empty
                           Else FAIL_HARD -> Error(ec, err, rollback=false)
                           Else FAIL_TRANSIENT -> Error(ec, err, retry=true)
                           Else -> raise Error(ec, cause=err)
                     */
                    var ec = e.Classify();
                    if (ec == ErrorClass.FailDocNotFound)
                    {
                        return null;
                    }

                    if (ec == ErrorClass.FailHard)
                    {
                        throw CreateError(this, ec)
                            .Cause(e)
                            .DoNotRollbackAttempt()
                            .Build();
                    }

                    if (ec == ErrorClass.FailTransient)
                    {
                        throw CreateError(this, ec)
                            .Cause(e)
                            .RetryTransaction()
                            .Build();
                    }

                    throw CreateError(this, ec)
                        .Cause(e)
                        .Build();
                }
            }
            catch (ErrorWrapperException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        private TransactionGetResult TransactionGetResultFromLookupIn(ICouchbaseCollection collection, string id,
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

        private async Task<(
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
            int fullDocumentIndex)> LookupDocWithMetada(ICouchbaseCollection collection, string id)
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

        public async Task<TransactionGetResult> Replace(TransactionGetResult doc, object content)
        {
            DoneCheck();
            CheckErrors();
            CheckExpiry();
            await CheckWriteWriteConflict(doc).CAF();
            InitAtrIfNeeded(doc.Collection, doc.Id);
            await SetAtrPendingIfFirstMutation(doc.Collection);

            // TODO: re-evaluate accessDeleted after CreateAsDeleted is implemented.
            return await CreateStagedReplace(doc, content, accessDeleted:false);
        }

        private async Task SetAtrPendingIfFirstMutation(ICouchbaseCollection collection)
        {
            if (_stagedMutations.Count == 0)
            {
                _ = await SetAtrPending(collection);
            }
        }

        private async Task<TransactionGetResult> CreateStagedReplace(TransactionGetResult doc, object content, bool accessDeleted)
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
                    .Durability(_effectiveDurabilityLevel );

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
                try
                {
                    HandleStagedRemoveOrReplaceError(ex, out var ec);

                    // Else -> Error(ec, err)
                    if (ex is ErrorWrapperException)
                    {
                        throw;
                    }

                    throw CreateError(this, ec)
                        .Cause(ex)
                        .Build();
                }
                catch (ErrorWrapperException toSave)
                {
                    // On any item above that raises an Error, before doing so, SaveErrorWrapper.
                    SaveErrorWrapper(toSave);
                    throw;
                }
            }
        }

        private void HandleStagedRemoveOrReplaceError(Exception ex, out ErrorClass ec)
        {
            ec = ex.Classify();

            switch (ec)
            {
                // FAIL_EXPIRY -> set ExpiryOvertimeMode and raise Error(ec, AttemptExpired(err),
                // raise=TRANSACTION_EXPIRED)
                case ErrorClass.FailExpiry:
                    _expirationOvertimeMode = true;
                    throw CreateError(this, ec)
                        .Cause(ex)
                        .Cause(new TransactionExpiredException("Expiration detected while attempting replace.",
                            ex))
                        .Build();

                // Else FAIL_DOC_NOT_FOUND || FAIL_CAS_MISMATCH -> Doc was modified in-between get and replace.
                // Error(ec, err, retry=true)
                // Else FAIL_DOC_NOT_FOUND [sic] || FAIL_TRANSIENT || FAIL_AMBIGUOUS
                // Error(ec, err, retry=true)
                case ErrorClass.FailDocNotFound:
                case ErrorClass.FailCasMismatch:
                case ErrorClass.FailTransient:
                case ErrorClass.FailAmbiguous:
                    throw CreateError(this, ec)
                        .Cause(ex)
                        .RetryTransaction()
                        .Build();

                // FAIL_HARD -> Error(ec, err, rollback=false)
                case ErrorClass.FailHard:
                    throw CreateError(this, ec)
                        .Cause(ex)
                        .DoNotRollbackAttempt()
                        .Build();
            }
        }

        private List<MutateInSpec> CreateMutationOps( string op)
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

        public async Task<TransactionGetResult> Insert(ICouchbaseCollection collection, string id, object content)
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
                var retryCount = 0;
                while (retryCount++ < _sanityRetryLimit)
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

                        var stagedMutation = new StagedMutation(getResult, contentBytes, StagedMutationType.Insert, mutateResult);
                        _stagedMutations.Add(stagedMutation);

                        return getResult;
                    }
                    catch (FeatureNotAvailableException e)
                    {
                        // If err is FeatureNotFoundException, then this cluster does not support creating shadow
                        // documents. (Unfortunately we cannot perform this check at the Transactions.create point,
                        // as we may not have a cluster config available then).
                        // Raise Error(ec=FAIL_OTHER, cause=err) to terminate the transaction.
                        throw CreateError(this, ErrorClass.FailOther)
                            .Cause(e).
                            Build();
                    }
                    catch (Exception ex)
                    {
                        // If in ExpiryOvertimeMode -> Error(FAIL_EXPIRY, AttemptExpired(err), rollback=false, raise=TRANSACTION_EXPIRED)
                        if (_expirationOvertimeMode)
                        {
                            throw CreateError(this, ErrorClass.FailExpiry)
                                .Cause(new AttemptExpiredException(this, "Cannot insert after transaction expired."))
                                .DoNotRollbackAttempt()
                                .RaiseException(ErrorWrapperException.FinalError.TransactionExpired)
                                .Build();
                        }

                        var ec = ex.Classify();
                        switch (ec)
                        {
                            // Else FAIL_EXPIRY -> set ExpiryOvertimeMode and raise Error(ec, AttemptExpired(err), raise=TRANSACTION_EXPIRED)
                            case ErrorClass.FailExpiry:
                                _expirationOvertimeMode = true;
                                throw CreateError(this, ec)
                                    .Cause(new AttemptExpiredException(this, "Attempt expired"))
                                    .RaiseException(ErrorWrapperException.FinalError.TransactionExpired)
                                    .Build();

                            // FAIL_AMBIGUOUS -> Ambiguously inserted marker documents can cause complexities during rollback and retry,
                            // so aim to resolve the ambiguity now by retrying this operation from the top of this section, after
                            // OpRetryDelay. If this op had succeeded then this will cause FAIL_DOC_EXISTS.
                            case ErrorClass.FailAmbiguous:
                                await Task.Delay(Transactions.OpRetryDelay).CAF();
                                break;

                            // FAIL_TRANSIENT -> Error(ec, err, retry=true)
                            case ErrorClass.FailTransient:
                                throw CreateError(this, ec, ex)
                                    .RetryTransaction()
                                    .Build();

                            // FAIL_HARD -> Error(ec, err, rollback=false)
                            case ErrorClass.FailHard:
                                throw CreateError(this, ec, ex)
                                    .DoNotRollbackAttempt()
                                    .Build();

                            // FAIL_CAS_MISMATCH -> We’re trying to overwrite an existing tombstone, and it’s changed. Could be an external actor (such as another transaction), or could be that a FAIL_AMBIGUOUS happened and actually succeeded. Either way, do the FAIL_DOC_ALREADY_EXISTS logic below again.
                            case ErrorClass.FailCasMismatch:
                                goto case ErrorClass.FailDocAlreadyExists;

                            case ErrorClass.FailDocAlreadyExists:
                                /*
                                 * FAIL_DOC_ALREADY_EXISTS -> There are multiple reasons we could end up here:
                                   Most likely, the document simply existed before, as a regular doc. In which case we want to fast fail.
                                   Another transaction B could have staged that document (either as a tombstone or not) then crashed in PENDING state. The cleanup process will not be able to resolve the document, it will only be able to remove B’s ATR entry. We can’t get blocked here, we want to overwrite the document if B’s ATR entry is removed.
                                   We got FAIL_AMBIGUOUS on the previous attempt (which could have been staging as a tombstone or not), which actually succeeded, so our retry of the insert hits this.
                                   We got FAIL_AMBIGUOUS but it actually failed and another transaction has managed to either stage or commit the same document (as a tombstone or not). Somewhat edge-casey, but means we can’t rely on some sort of isAmbiguityResolution flag.
                                   (In both FAIL_AMBIGUOUS cases, we want to proceed as successful only if the document is staged with this transaction’s metadata. Else we want to fast fail.)
                                   The logic to resolve all these cases is to get the document, and check its metadata and tombstone status to see whether to proceed.
                                 */
                                try
                                {
                                    await _testHooks.BeforeGetDocInExistsDuringStagedInsert(this, id).CAF();
                                    var docWithMeta = await LookupDocWithMetada(collection, id).CAF();
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
                                        break;
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
                                        break;
                                    }
                                }
                                catch (Exception exHandleDocExists)
                                {
                                    var ecHandleDocExists = ex.Classify();
                                    switch (ecHandleDocExists)
                                    {
                                        // FAIL_DOC_NOT_FOUND -> The doc did exist, and now doesn’t.
                                        // (This is extremely unlikely given that we’re fetching tombstones.)
                                        // Error(ec, retry=true)
                                        // FAIL_PATH_NOT_FOUND -> This is what will happen if the doc has been deleted, or if it’s simply a normal tombstone.
                                        // We will get the tombstone and it will be missing the “txn” xattr metadata.
                                        // Error(ec, retry=true)
                                        // FAIL_TRANSIENT -> Let’s try all this again.
                                        // Error(ec, retry=true)
                                        case ErrorClass.FailDocNotFound:
                                        case ErrorClass.FailPathNotFound:
                                        case ErrorClass.FailTransient:
                                            throw CreateError(this, ecHandleDocExists, exHandleDocExists)
                                                .RetryTransaction()
                                                .Build();

                                        // Else -> Bailout. Error(ec, cause=err)
                                        default:
                                            throw CreateError(this, ecHandleDocExists, exHandleDocExists)
                                                .Build();
                                    }
                                }
                        }
                    }
                }

                // This should not be hit under normal circumstances.  Expiration will happen first.
                throw CreateError(this, ErrorClass.FailOther)
                    .Cause(new InvalidOperationException("Retried past sanity limit.  Possible infinite loop."))
                    .RaiseException(ErrorWrapperException.FinalError.TransactionFailed)
                    .Build();
            }
            catch (ErrorWrapperException toSave)
            {
                SaveErrorWrapper(toSave);
                throw;
            }
        }

        private IEnumerable<StagedMutation> StagedInserts  =>
            _stagedMutations.Where(sm => sm.Type == StagedMutationType.Insert);

        private IEnumerable<StagedMutation> StagedReplaces => _stagedMutations.Where(sm => sm.Type == StagedMutationType.Replace);
        private IEnumerable<StagedMutation> StagedRemoves => _stagedMutations.Where(sm => sm.Type == StagedMutationType.Remove);


        private async Task<IMutateInResult> SetAtrPending(ICouchbaseCollection collection)
        {
            _atrId = _atrId ?? throw new InvalidOperationException("atrId is not present");

            try
            {
                await _testHooks.BeforeAtrPending(this);

                var prefix = $"attempts.{_attemptId}";
                var t1 = _overallContext.StartTime;
                var t2 = DateTimeOffset.UtcNow;
                var tElapsed = t2 - t1;
                var tc = _config.ExpirationTime;
                var tRemaining = tc - tElapsed;
                var exp = (ulong)Math.Max(Math.Min(tRemaining.TotalMilliseconds, tc.TotalMilliseconds), 0);

                var mutateInResult = await collection.MutateInAsync(_atrId, specs =>
                        specs.Insert($"{prefix}.{TransactionFields.AtrFieldTransactionId}", _overallContext.TransactionId,
                                createPath: true, isXattr: true)
                            .Insert($"{prefix}.{TransactionFields.AtrFieldStatus}", AttemptStates.PENDING.ToString(), createPath: false, isXattr: true)
                            .Insert($"{prefix}.{TransactionFields.AtrFieldStartTimestamp}", MutationMacro.Cas)
                            .Insert($"{prefix}.{TransactionFields.AtrFieldExpiresAfterMsecs}", exp, createPath: false, isXattr: true),
                    opts => opts.StoreSemantics(StoreSemantics.Upsert)
                ).CAF();

                var lookupInResult = await collection.LookupInAsync(_atrId,
                    specs => specs.Get($"{prefix}.{TransactionFields.AtrFieldStartTimestamp}", isXattr: true));
                var fetchedCas = lookupInResult.ContentAs<string>(0);
                var getResult = await collection.GetAsync(_atrId).CAF();
                var atr = getResult.ContentAs<dynamic>();
                await _testHooks.AfterAtrPending(this);
                _state = AttemptStates.PENDING;
                return mutateInResult;
            }
            catch (Exception e)
            {
                // TODO: Handle error per spec
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

        public async Task Remove(TransactionGetResult doc)
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
                    specs.Add(MutateInSpec.Upsert(TransactionFields.StagedData, TransactionFields.StagedDataRemoveKeyword, isXattr: true));

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

                        // Commit+rollback: Want to delete the staged empty doc
                        // However this is hard in practice.  If we remove from stagedInsert and add to
                        // stagedRemove then commit will work fine, but rollback will not remove the doc.
                        // So, fast fail this scenario.
                        throw new InvalidOperationException($"doc {Redactor.UserData(doc.Id)} is being removed after being inserted in the same txn.");
                    }

                    var stagedRemove = new StagedMutation(doc, ActiveTransactionRecord.RemovePlaceholderBytes, StagedMutationType.Remove, updatedDoc);
                    _stagedMutations.Add(stagedRemove);
                }
                catch (Exception ex)
                {
                    HandleStagedRemoveOrReplaceError(ex, out var ec);

                    // Else -> Error(ec, err)
                    if (ex is ErrorWrapperException)
                    {
                        throw;
                    }

                    throw CreateError(this, ec)
                        .Cause(ex)
                        .Build();
                }
            }
            catch (ErrorWrapperException toSave)
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
                    await Commit().CAF();
                    break;
            }
        }
        public async Task Commit()
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Commit
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

            try
            {
                await SetAtrCommit().CAF();
                await UnstageDocs().CAF();
                await SetAtrComplete().CAF();
            }
            catch (Exception e)
            {
                throw;
            }

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
            catch (Exception e)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
FAIL_HARD -> Error(ec, err, rollback=false)
Else -> Setting the ATR to COMPLETED is purely a cleanup step, there’s no need to retry it until expiry. Simply return success (leaving state at COMMITTED).
Note this will leave TransactionResult::unstageCompleted() returning false, even though a use of TransactionResult::mutationState() would be fine. Fixing this issue would require the complexity of maintaining additional mutable state. We will monitor if this is a problem in real deployments and can consider returning to this.
A FAIL_AMBIGUOUS could leave the ATR state as COMPLETED but the in-memory state as COMMITTED. This shouldn’t cause any problems.
                 */

                                    throw;
            }
        }

        private async Task UnstageDocs()
        {
            foreach (var sm in _stagedMutations)
            {
                switch (sm.Type)
                {
                    case StagedMutationType.Remove:
                        await UnstageRemove(sm, ambiguityResolutionMode: false).CAF();
                        break;
                    case StagedMutationType.Insert:
                        await UnstageMutation(sm, casZeroMode: false, insertMode: true, ambiguityResolutionMode: false).CAF();
                        break;
                    case StagedMutationType.Replace:
                        await UnstageMutation(sm, casZeroMode: false, insertMode: false, ambiguityResolutionMode: false)
                            .CAF();
                        break;
                    default:
                        throw new InvalidOperationException($"Cannot un-stage transaction mutation of type {sm.Type}");
                }
            }
        }

        private async Task UnstageRemove(StagedMutation sm, bool ambiguityResolutionMode)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Removes
            try
            {
                await _testHooks.BeforeDocRemoved(this, sm.Doc.Id).CAF();
                CheckExpiry();
                await sm.Doc.Collection.RemoveAsync(sm.Doc.Id);
                await _testHooks.AfterDocRemovedPreRetry(this, sm.Doc.Id).CAF();
            }
            catch (Exception e)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
                   If in ExpiryOvertimeMode -> Error(ec, AttemptExpired(err), rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT)
                   Else FAIL_AMBIGUOUS -> Retry this section from the top, after OpRetryDelay, with ambiguityResolutionMode=true.
                   Else FAIL_DOC_NOT_FOUND -> Doc was removed in-between stage and unstaging.
                   Either we are in in ambiguityResolutionMode mode, and our second attempt found that the first attempt was successful. Or, another actor has removed the document. Either way, we cannot continue as the returned mutationTokens won’t contain this removal. Raise Error(ec, err, rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT).
                   Note: The ambiguityResolutionMode logic is redundant currently. But we may have better handling for this in the future, e.g. a mode where the application can specify that mutationTokens isn’t important for this transaction.
                   Else FAIL_HARD -> Error(ec, err, rollback=false)
                   Else -> Raise Error(ec, cause=err, rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT). This will result in success being return to the application, but result.unstagingCompleted() will be false. See “Unstaging Inserts & Replaces” for the logic behind this.
                 */
                throw;
            }

            _finalMutations.Add(sm.MutationResult.MutationToken);
            await _testHooks.AfterDocRemovedPostRetry(this, sm.Doc.Id).CAF();
        }

        private async Task UnstageMutation(StagedMutation sm, bool casZeroMode = false, bool insertMode = false, bool ambiguityResolutionMode = false)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Inserts-and-Replaces-Protocol-20-version
            await _testHooks.BeforeDocCommitted(this, sm.Doc.Id).CAF();
            CheckExpiry();

            try
            {
                IMutationResult mutateResult;
                var finalDoc = _transcoder.Serializer.Deserialize<JObject>(sm.Content);
                if (insertMode)
                {
                    // TODO: the spec has been updated.
                    // Implement https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A#Unstaging-Inserts-and-Replaces-Protocol-20-version
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
                            opts => opts.Cas( casZeroMode ? 0 : sm.Doc.Cas).StoreSemantics(StoreSemantics.Replace))
                        .CAF();
                }

                if (mutateResult?.MutationToken != null)
                {
                    _finalMutations.Add(mutateResult.MutationToken);
                }

                await _testHooks.AfterDocCommittedBeforeSavingCas(this, sm.Doc.Id);
            }
            catch (Exception e)
            {
                /* On error err (from any of the preceding items in this section), classify as error class ec then:
If in ExpiryOvertimeMode -> Error(ec, AttemptExpired(err), rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT)
Else FAIL_AMBIGUOUS -> Retry this section from the top, after OpRetryDelay, with ambiguityResolutionMode=true and the current casZero.
Else FAIL_CAS_MISMATCH -> Doc has changed in-between stage and unstaging.
If ambiguityResolutionMode=true, then our previous attempt likely succeeded. Unfortunately, we cannot continue as our returned mutationTokens wouldn’t include this mutation. Raise Error(ec, err, rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT).
Else, another actor has changed the document, breaking the co-operative model (it should not be possible for another transaction to do this). Publish an IllegalDocumentState event to the application (the details of event publishing are platform-specific: on Java, it uses the Java SDK’s event bus). Run this section again from the top, with casZeroMode=true and the current ambiguityResolutionMode and insertMode.
Else FAIL_DOC_NOT_FOUND -> Doc was removed in-between stage and unstaging.
Something has broken the co-operative model. The transaction must commit, so we will insert the document.
Publish an IllegalDocumentState event to the application.
Run this section again from the top, after OpRetryDelay, with insertMode=true, and the current ambiguityResolutionMode and casZeroMode. This will insert the document.
Else FAIL_HARD -> Error(ec, err, rollback=false)
Else FAIL_DOC_ALREADY_EXISTS ->
If in ambiguityResolutionMode, probably we inserted a doc over a shadow document, and it raised FAIL_AMBIGUOUS but was successful. Resolving this is perhaps impossible - we don’t want to retry the operation, as the document is now committed and another transaction (or KV op) may have started on it. We could reread the doc and check it contains the expected content - but again it may have been involved in another transaction. So, raise Error(ec, cause=err, rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT).
Else, seems the co-operative model has been broken. The transaction must commit, so we will replace the document.
Publish an IllegalDocumentState event to the application.
Run this section again from the top, after OpRetryDelay, with insertMode=false, the current ambiguityResolutionMode, and casZeroMode=true.
Else -> Raise Error(ec, cause=err, rollback=false, raise=TRANSACTION_FAILED_POST_COMMIT). This will result in success being return to the application, but result.unstagingCompleted() will be false.
The transaction is conceptually committed but unstaging could not complete. Since we have reached the COMMIT point, all transactions will see the post-transaction values now. For many application cases (e.g. those not doing RYOWs) this is not an error at all, and it seems unwise to repeatedly retry this operation if the cluster is currently overwhelmed. Returning success now will make transactions more resilient in the face of e.g. rebalances. The commit will still be completed by the async cleanup process later.
                */
                throw;
            }
        }

        private async Task SetAtrCommit(bool ambiguityResolutionMode = false)
        {
            try
            {
                CheckExpiry();
                await _testHooks.BeforeAtrCommit(this).CAF();
                var prefix = $"attempts.{_attemptId}";

                var inserts = new JArray(StagedInserts.Select(sm => sm.ForAtr()));
                var replaces = new JArray(StagedReplaces.Select(sm => sm.ForAtr()));
                var removes = new JArray(StagedRemoves.Select(sm => sm.ForAtr()));
                var specs = new MutateInSpec[]
                {
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}", AttemptStates.COMMITTED.ToString(), isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStartCommit}", MutationMacro.Cas),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsInserted}", inserts, isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsReplaced}", replaces, isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsRemoved}", removes, isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldPendingSentinel}", 0, isXattr: true)
                };


                var mutateInResult = await _atrCollection
                    .MutateInAsync(_atrId, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();

                await _testHooks.AfterAtrCommit(this).CAF();
                _state = AttemptStates.COMMITTED;
            }
            catch (Exception e)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
FAIL_EXPIRY ->
Protocol 1:
If ambiguityResolutionMode==true, we were unable to attain clarity over whether we reached committed or not. Set ExpiryOvertimeMode and raise Error(ec, AttemptExpired(err), raise=TRANSACTION_COMMIT_AMBIGUOUS)
Else, we unambiguously were not able to set the ATR to Committed. Set ExpiryOvertimeMode and raise Error(ec, AttemptExpired(err), raise=TRANSACTION_EXPIRED)
Protocol 2:
We unambiguously were not able to set the ATR to Committed. Set ExpiryOvertimeMode and raise Error(ec, AttemptExpired(err), raise=TRANSACTION_EXPIRED)
Else FAIL_AMBIGUOUS ->
Ambiguity resolution is very important here, and we cannot proceed until we are certain. E.g. if the op succeeded then we are past the point of no return and must commit.
Protocol 1:
Repeat the SetATRCommit step from the top to retry the idempotent commit step, with ambiguityResolutionMode=true, after waiting OpRetryDelay.
Protocol 2:
Perform the SetATRCommit Ambiguity Resolution logic.
Else FAIL_HARD -> Error(ec, err, rollback=false)
Else FAIL_TRANSIENT -> Error(ec, err, retry=true)
Else -> Error(ec, err)
                 */
                throw;
            }
        }

        private async Task SetAtrAborted(bool isAppRollback)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#SetATRAborted
            try
            {
                // TODO: handle ExpirationOvertimeMode
                CheckExpiry();

                await _testHooks.BeforeAtrAborted(this).CAF();
                var prefix = $"attempts.{_attemptId}";

                var inserts = new JArray(StagedInserts.Select(sm => sm.ForAtr()));
                var replaces = new JArray(StagedReplaces.Select(sm => sm.ForAtr()));
                var removes = new JArray(StagedRemoves.Select(sm => sm.ForAtr()));
                var specs = new MutateInSpec[]
                {
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}", AttemptStates.ABORTED.ToString(), isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldTimestampRollbackStart}", MutationMacro.Cas),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsInserted}", inserts, isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsReplaced}", replaces, isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldDocsRemoved}", removes, isXattr: true),
                };

                var mutateInResult = await _atrCollection
                    .MutateInAsync(_atrId, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();


                await _testHooks.AfterAtrAborted(this).CAF();
                _state = AttemptStates.ABORTED;
            }
            catch (Exception)
            {
                /*
                On error err (from any of the preceding items in this section), classify as error class ec then:
                   If in ExpiryOvertimeMode -> Error(ec, cause=AttemptExpired(err), rollback=false, raise=TRANSACTION_EXPIRED)
                   Else if FAIL_EXPIRY -> set ExpiryOvertimeMode and retry operation, after waiting OpRetryBackoff. We want to make one further attempt to complete the rollback.
                   Else FAIL_PATH_NOT_FOUND -> Perhaps we’re trying to rollback an ATR entry after failing trying to create it. Perhaps, the cleanup process has removed the entry, as it was expired. Neither of these should happen, so we should bailout as we’re now in a strange state. Error(ec, cause=ActiveTransactionRecordEntryNotFound, rollback=false)
                   Else FAIL_DOC_NOT_FOUND -> The ATR has been deleted, or we’re trying to rollback an attempt that failed to create a new ATR. Neither should happen, so bailout. Error(ec, cause=ActiveTransactionRecordNotFound, rollback=false)
                   Else FAIL_ATR_FULL -> Bailout to reduce pressure on ATRs. Error(ec, cause=ActiveTransactionRecordFull, rollback=false)
                   Else FAIL_HARD -> Error(ec, err, rollback=false)
                   Else -> Default current logic is that rollback will continue in the event of failures until expiry. Retry operation, after waiting OpRetryBackoff. Takes care of FAIL_AMBIGUOUS.
                 *
                 */
                throw;
            }
        }

        private async Task SetAtrRolledBack()
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#SetATRRolledBack
            try
            {
                CheckExpiry();
                await _testHooks.BeforeAtrRolledBack(this).CAF();
                var prefix = $"attempts.{_attemptId}";

                var specs = new MutateInSpec[]
                {
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldStatus}", AttemptStates.ROLLED_BACK.ToString(), isXattr: true),
                    MutateInSpec.Upsert($"{prefix}.{TransactionFields.AtrFieldTimestampRollbackComplete}", MutationMacro.Cas),
                };

                var mutateInResult = await _atrCollection
                    .MutateInAsync(_atrId, specs, opts => opts.StoreSemantics(StoreSemantics.Replace)).CAF();

                await _testHooks.AfterAtrRolledBack(this).CAF();
                _state = AttemptStates.ROLLED_BACK;
            }
            catch (Exception)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
                   If in ExpiryOvertimeMode -> Error(FAIL_EXPIRY, cause=AttemptExpired(err), rollback=false, raise=TRANSACTION_EXPIRED)
                   Else if FAIL_EXPIRY -> set ExpiryOvertimeMode and retry operation, after waiting OpRetryBackoff. We want to make one further attempt to complete the rollback.
                   Else FAIL_PATH_NOT_FOUND -> Perhaps, the cleanup process has removed the entry, as it was expired (though this is unlikely). Continue as though success.
                   Else FAIL_DOC_NOT_FOUND -> The ATR has been deleted, or we’re trying to rollback an attempt that failed to create a new ATR. Neither should happen, so bailout. Error(ec, cause=ActiveTransactionRecordNotFound, rollback=false)
                   Else FAIL_HARD -> Error(ec, err, rollback=false)
                   Else -> Default current logic is that rollback will continue in the event of failures until expiry. Retry operation, after waiting OpRetryBackoff. Takes care of FAIL_AMBIGUOUS.
                 */
                throw;
            }
        }

        public Task Rollback() => this.RollbackInternal(true);

        public Task Defer() => throw new NotImplementedException();

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
            https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#Rollback-Staged-Insert
            try
            {
                CheckExpiry();
                await _testHooks.BeforeRollbackDeleteInserted(this, sm.Doc.Id).CAF();
                var mutateInResult = sm.Doc.Collection.MutateInAsync(sm.Doc.Id, specs =>
                    specs
                        // NCBC-2639
                        ////.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null,
                        ////    isXattr: true)
                        .Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)).CAF();

                await _testHooks.AfterRollbackDeleteInserted(this, sm.Doc.Id).CAF();
            }
            catch (Exception)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
                   If ExpiryOvertimeMode -> time to bailout. RaiseError(ec, AttemptExpired(err), raise=TRANSACTION_EXPIRED).
                   Else FAIL_EXPIRY -> Set ExpiryOvertimeMode and retry operation, after waiting OpRetryBackoff.
                   Else FAIL_DOC_NOT_FOUND -> Possibly we retried the op on FAIL_AMBIGUOUS and that op had succeeded. Perhaps something odd has happened and async cleanup has rolled back this doc while we’ve been trying to. Either way, our work on this document is done. Continue as success.
                   Protocol 2.0 version:
                   Else FAIL_PATH_NOT_FOUND -> same logic as FAIL_DOC_NOT_FOUND for same reason.
                   Else FAIL_CAS_MISMATCH -> Either the co-operative model has been broken, or we’re retrying on a previous FAIL_AMBIGUOUS that actually succeeded. We could resolve the ambiguity here, but it’s somewhat expensive (would require reading the doc), and it’s only rollback cleanup. So instead bailout and leave it for the cleanup process Error(ec, err, rollback=false)
                   Else FAIL_HARD -> Error(ec, err, rollback=false)
                   Else -> Default current logic is that rollback will continue in the event of failures until expiry. Retry operation, after waiting OpRetryBackoff.
                 */
                throw;
            }
        }

        private async Task RollbackStagedReplaceOrRemove(StagedMutation sm)
        {
            // https://hackmd.io/Eaf20XhtRhi8aGEn_xIH8A?view#Rollback-Staged-Replace-or-Remove
            try
            {
                CheckExpiry();
                await _testHooks.BeforeDocRolledBack(this, sm.Doc.Id).CAF();
                var mutateInResult = sm.Doc.Collection.MutateInAsync(sm.Doc.Id, specs =>
                    specs
                        // NCBC-2639
                        ////.Upsert(TransactionFields.TransactionInterfacePrefixOnly, (string?)null,
                        ////    isXattr: true)
                        .Remove(TransactionFields.TransactionInterfacePrefixOnly, isXattr: true)).CAF();
                await _testHooks.AfterRollbackReplaceOrRemove(this, sm.Doc.Id).CAF();
            }
            catch (Exception)
            {
                /*
                 * On error err (from any of the preceding items in this section), classify as error class ec then:
                   If ExpiryOvertimeMode -> time to bailout. RaiseError(ec, AttemptExpired(err), raise=TRANSACTION_EXPIRED).
                   Else FAIL_EXPIRY -> Set ExpiryOvertimeMode and retry operation, after waiting OpRetryBackoff.
                   Else FAIL_PATH_NOT_FOUND -> The transactional metadata already doesn’t exist. Possibly we retried the op on FAIL_AMBIGUOUS and that op had succeeded. Perhaps something odd has happened and async cleanup has rolled back this doc while we’ve been trying to. Either way, our work on this document is done. Continue as success.
                   Else FAIL_DOC_NOT_FOUND -> Should not happen, likely means the co-operative model has been broken. But as it’s rollback and no mutations are going to lost, do not raise an event. Error(ec, err, rollback=false)
                   Protocol 2.0: Else FAIL_CAS_MISMATCH -> Either the co-operative model has been broken, or we’re retrying on a previous FAIL_AMBIGUOUS that actually succeeded. We could resolve the ambiguity here, but it’s somewhat expensive (would require reading the doc), and it’s only rollback cleanup. So instead bailout and leave it for the cleanup process Error(ec, err, rollback=false)
                   Else FAIL_HARD -> Error(ec, err, rollback=false)
                   Else -> Default current logic is that rollback will continue in the event of failures until expiry. Retry operation, after waiting OpRetryBackoff.
                 */
                throw;
            }
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

        protected void CheckExpiry()
        {
            if (_overallContext.IsExpired)
            {
                _expirationOvertimeMode = true;
                throw CreateError(this, ErrorClass.FailExpiry)
                    .RaiseException(ErrorWrapperException.FinalError.TransactionExpired)
                    .Build();
            }
        }

        protected async Task CheckWriteWriteConflict(TransactionGetResult gr)
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

        internal void SaveErrorWrapper(ErrorWrapperException ex)
        {
            _previousErrors.TryAdd(ex.ExceptionNumber, ex);
        }

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
