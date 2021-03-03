using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading.Tasks;

namespace Couchbase.Transactions.Internal.Test
{
    /// <summary>
    /// Protected hooks purely for testing purposes.  If you're an end-user looking at these for any reason, then
    /// please contact us first about your use-case: we are always open to adding good ideas into the transactions
    /// library.
    /// </summary>
    /// <remarks>All methods have default no-op implementations.</remarks>
    internal interface ITestHooks
    {
        public const string HOOK_ROLLBACK = "rollback";
        public const string HOOK_GET = "get";
        public const string HOOK_INSERT = "insert";
        public const string HOOK_REPLACE = "replace";
        public const string HOOK_REMOVE = "remove";
        public const string HOOK_BEFORE_COMMIT = "commit";
        public const string HOOK_ABORT_GET_ATR = "abortGetAtr"; // No references in Java code.
        public const string HOOK_ROLLBACK_DOC = "rollbackDoc";
        public const string HOOK_DELETE_INSERTED = "deleteInserted";
        public const string HOOK_CREATE_STAGED_INSERT = "createdStagedInsert";
        public const string HOOK_INSERT_QUERY = "insertQuery";
        public const string HOOK_REMOVE_DOC = "removeDoc";
        public const string HOOK_COMMIT_DOC = "commitDoc";
        public const string HOOK_QUERY = "query";
        public const string HOOK_ATR_COMMIT = "atrCommit";
        public const string HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION = "atrCommitAmbiguityResolution";
        public const string HOOK_ATR_ABORT = "atrAbort";
        public const string HOOK_ATR_ROLLBACK_COMPLETE = "atrRollbackComplete";
        public const string HOOK_ATR_PENDING = "atrPending";
        public const string HOOK_ATR_COMPLETE = "atrComplete";
        public const string HOOK_CHECK_WRITE_WRITE_CONFLICT = "checkATREntryForBlockingDoc";

        Task<int?> BeforeAtrCommit(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterAtrCommit(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeDocCommitted(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeDocRolledBack(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocCommittedBeforeSavingCas(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocCommitted(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocsCommitted(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeDocRemoved(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocRemovedPreRetry(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocRemovedPostRetry(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterDocsRemoved(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeAtrPending(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterAtrPending(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterAtrComplete(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeAtrComplete(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeAtrRolledBack(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterGetComplete(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeRollbackDeleteInserted(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterStagedReplaceComplete(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterStagedRemoveComplete(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeStagedInsert(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeStagedRemove(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeStagedReplace(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterStagedInsertComplete(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeGetAtrForAbort(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> BeforeAtrAborted(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterAtrAborted(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterAtrRolledBack(AttemptContext self) => Task.FromResult<int?>(0);

        Task<int?> AfterRollbackReplaceOrRemove(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> AfterRollbackDeleteInserted(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeRemovingDocDuringStagedInsert(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeCheckAtrEntryForBlockingDoc(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeDocGet(AttemptContext self, string id) => Task.FromResult<int?>(0);

        Task<int?> BeforeGetDocInExistsDuringStagedInsert(AttemptContext self, string id) => Task.FromResult<int?>(0);

        bool HasExpiredClientSideHook(AttemptContext self, string place, string? docId) => false;
        Task<int?> BeforeAtrCommitAmbiguityResolution(AttemptContext attemptContext) => Task.FromResult<int?>(0);

        Task<string?> AtrIdForVBucket(AttemptContext self, int vbucketId) => Task.FromResult<string?>(null);
    }

    /// <summary>
    /// Implementation of ITestHooks that relies on default interface implementation.
    /// </summary>
    internal class DefaultTestHooks : ITestHooks
    {
        public static readonly ITestHooks Instance = new DefaultTestHooks();
    }

    /// <summary>
    /// Implementation of ITestHooks that allows individual delegates per hook.
    /// </summary>
    public class DelegateTestHooks : ITestHooks
    {
        public Func<AttemptContext, string, Task<int?>> BeforeDocGetImpl { get; set; } = DefaultTestHooks.Instance.BeforeDocGet;
        public Task<int?> BeforeDocGet(AttemptContext self, string id) => BeforeDocGetImpl(self, id);

        public Func<AttemptContext, string, Task<int?>> BeforeDocCommittedImpl { get; set; } =
            DefaultTestHooks.Instance.BeforeDocCommitted;
        public Task<int?> BeforeDocCommitted(AttemptContext self, string id) => BeforeDocCommittedImpl(self, id);

        public Func<AttemptContext, Task<int?>> BeforeAtrCommitImpl { get; set; } =
            DefaultTestHooks.Instance.BeforeAtrCommit;
        public Task<int?> BeforeAtrCommit(AttemptContext self) => BeforeAtrCommitImpl(self);

        public Func<AttemptContext, string, Task<int?>> AfterStagedReplaceCompleteImpl { get; set; } =
            DefaultTestHooks.Instance.AfterStagedReplaceComplete;

        public Task<int?> AfterStagedReplaceComplete(AttemptContext self, string id) =>
            AfterStagedReplaceCompleteImpl(self, id);
    }
}
