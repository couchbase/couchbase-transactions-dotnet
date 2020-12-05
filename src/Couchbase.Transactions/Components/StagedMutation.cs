using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.Core;
using Couchbase.KeyValue;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Components
{
    internal class StagedMutation
    {
        public TransactionGetResult Doc { get; }
        public object Content { get; }
        public StagedMutationType Type { get; }
        public MutationToken MutationToken { get; }

        public StagedMutation(TransactionGetResult doc, object content, StagedMutationType type, MutationToken mutationToken)
        {
            Doc = doc;
            Content = content;
            Type = type;
            MutationToken = mutationToken;
        }

        public JObject ForAtr() => new JObject(
            new JProperty(TransactionFields.AtrFieldPerDocId, Doc.Id),
            new JProperty(TransactionFields.AtrFieldPerDocBucket, Doc.Collection.Scope.Bucket.Name),
            new JProperty(TransactionFields.AtrFieldPerDocScope, Doc.Collection.Scope.Name),
            new JProperty(TransactionFields.AtrFieldPerDocCollection, Doc.Collection.Name)
        );

        public DocRecord AsDocRecord() => new DocRecord(
            bkt: Doc.Collection.Scope.Bucket.Name,
            scp: Doc.Collection.Scope.Name,
            col: Doc.Collection.Name,
            id: Doc.Id);
    }

    internal enum StagedMutationType
    {
        Undefined = 0,
        Insert = 1,
        Remove = 2,
        Replace = 3
    }
}
