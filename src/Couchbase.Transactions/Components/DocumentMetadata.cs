using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Transactions.Components
{
    public class DocumentMetadata
    {
        public DocumentMetadata(string? cas, string? revId, ulong? expTime) => (Cas, RevId, ExpTime) = (cas, revId, expTime);

        public string? Cas { get; }
        public string? RevId { get; }
        public ulong? ExpTime { get; }
    }
}
