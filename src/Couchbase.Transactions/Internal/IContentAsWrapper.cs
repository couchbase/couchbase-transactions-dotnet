using System;
using System.Collections.Generic;
using System.Text;
using Couchbase.KeyValue;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Internal
{
    /// <summary>
    /// An interface for deferring ContentAs calls to their original source to avoid byte[]/json/string conversion in the middle.
    /// </summary>
    internal interface IContentAsWrapper
    {
        T ContentAs<T>();
    }

    internal class JObjectContentWrapper : IContentAsWrapper
    {
        private readonly object _originalContent;

        public JObjectContentWrapper(object originalContent)
        {
            _originalContent = originalContent;
        }

        public T ContentAs<T>() =>
            _originalContent is T asTyped ? asTyped : JObject.FromObject(_originalContent).ToObject<T>();
    }

    internal class LookupInContentAsWrapper : IContentAsWrapper
    {
        private readonly ILookupInResult _lookupInResult;
        private readonly int _specIndex;

        public LookupInContentAsWrapper(ILookupInResult lookupInResult, int specIndex)
        {
            _lookupInResult = lookupInResult;
            _specIndex = specIndex;
        }

        public T ContentAs<T>() => _lookupInResult.ContentAs<T>(_specIndex);
    }
}
