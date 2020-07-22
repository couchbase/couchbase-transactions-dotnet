using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Couchbase.Transactions.Support
{
    public enum AttemptStates
    {
        [Description(AttemptStateExtensions.NothingWritten)]
        NothingWritten = 0,

        [Description(AttemptStateExtensions.Pending)]
        Pending = 1,

        [Description(AttemptStateExtensions.Aborted)]
        Aborted = 2,

        [Description(AttemptStateExtensions.Committed)]
        Committed = 3,

        [Description(AttemptStateExtensions.Completed)]
        Completed = 4,

        [Description(AttemptStateExtensions.RolledBack)]
        RolledBack = 5
    }

    /// <summary>
    /// Hackish boilerplate to make up for .NET's simplistic enums compared to the CB JVM Clients.
    /// </summary>
    internal static class AttemptStateExtensions
    {
        public const string NothingWritten = "NOTHING_WRITTEN";
        public const string Pending = "PENDING";
        public const string Aborted = "ABORTED";
        public const string Committed = "COMITTED";
        public const string Completed = "COMPLETED";
        public const string RolledBack = "ROLLED_BACK";

        public static string FullName(this AttemptStates status)
        {
            return status switch
            {
                AttemptStates.NothingWritten => NothingWritten,
                AttemptStates.Pending => Pending,
                AttemptStates.Aborted => Aborted,
                AttemptStates.Committed => Committed,
                AttemptStates.Completed => Completed,
                AttemptStates.RolledBack => RolledBack,
                _ => status.ToString(),
            };
        }
    }
}
