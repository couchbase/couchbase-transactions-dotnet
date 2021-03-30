using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Couchbase.Core;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Log;
using Couchbase.Transactions.Support;
using Newtonsoft.Json;

namespace Couchbase.Transactions
{
    public class TransactionAttempt
    {
        public TimeSpan TimeTaken { get; internal set; }

        [JsonIgnore]
        internal DocRecord? AtrRecord { get; set; }

        public string? AtrCollectionName => AtrRecord?.CollectionName;
        public string? AtrScopeName => AtrRecord?.ScopeName;
        public string? AtrBucketName => AtrRecord?.BucketName;

        public string? AtrId => AtrRecord?.Id;
        public AttemptStates FinalState { get; internal set; }
        public string AttemptId { get; internal set; } = string.Empty;
        public IEnumerable<string> StagedInsertedIds { get; internal set; } = Enumerable.Empty<string>();
        public IEnumerable<string> StagedReplaceIds { get; internal set; } = Enumerable.Empty<string>();
        public IEnumerable<string> StagedRemoveIds { get; internal set; } = Enumerable.Empty<string>();
        public Exception? TerminatedByException { get; internal set; } = null;
        public IEnumerable<MutationToken> MutationTokens { get; internal set; } = Enumerable.Empty<MutationToken>();

        public override string ToString()
        {
            const int logDeferCharsToLog = 5; // TODO: move constant to equivalent of LogDefer class in Java code.
            var sb = new StringBuilder();
            sb.Append(nameof(TransactionAttempt)).Append("{id=").Append(AttemptId.SafeSubstring(logDeferCharsToLog));
            sb.Append(",state=").Append(FinalState);
            sb.Append(",atrScp=").Append(AtrScopeName ?? "<none>");
            sb.Append(",atrColl=").Append(AtrCollectionName ?? "<none>");
            sb.Append(",atrBkt=").Append(AtrBucketName ?? "<none>");
            sb.Append("/atrId=").Append(AtrId ?? "<none>");
            sb.Append("}");
            return sb.ToString();
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
