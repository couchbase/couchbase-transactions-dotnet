////using System;
////using System.Collections.Generic;
////using System.Diagnostics.CodeAnalysis;
////using System.Text;
////using Couchbase.Core;
////using Couchbase.KeyValue;

////namespace Couchbase.Transactions
////{
////    public interface ITransactionGetResult : IGetResult
////    {
////        string Id { get; }
////    }

////    internal class TransactionGetResult : ITransactionGetResult
////    {
////        private readonly IGetResult _getResult;

////        public TransactionGetResult([NotNull] IGetResult getResult, [NotNull] string docId)
////        {
////            _getResult = getResult;
////            Id = docId;
////        }

////        public string Id { get; }
////        public ulong Cas => _getResult.Cas;
////        public void Dispose() => _getResult.Dispose();

////        public T ContentAs<T>() => _getResult.ContentAs<T>();

////        public TimeSpan? Expiry => _getResult.Expiry;
////    }

////    internal class TransactionMutateInResult : ITransactionGetResult, IMutateInResult
////    {
////        private readonly IMutateInResult _mutateResult;
////        private readonly int _fullDocIndex;

////        public TransactionMutateInResult([NotNull] IMutateInResult mutateResult, [NotNull] string docId, int fullDocIndex)
////        {
////            _mutateResult = mutateResult;
////            _fullDocIndex = fullDocIndex;
////            Id = docId;
////        }

////        public ulong Cas => _mutateResult.Cas;

////        public MutationToken MutationToken
////        {
////            get => _mutateResult.MutationToken;
////            set
////            {
////                _mutateResult.MutationToken = value;
////            }
////        }

////        public T ContentAs<T>(int index) => _mutateResult.ContentAs<T>(index);

////        public void Dispose() => (_mutateResult as IDisposable)?.Dispose();

////        public T ContentAs<T>() => ContentAs<T>(_fullDocIndex);

////        public TimeSpan? Expiry => null; // FIXME

////        public string Id { get; }
////    }

////    internal static class TransactionGetResultExtensions
////    {
////        internal static void Deconstruct(this ITransactionGetResult tgr, out string id, out ulong cas, out TimeSpan? expiry)
////        {
////            id = tgr.Id;
////            cas = tgr.Cas;
////            expiry = tgr.Expiry;
////        }
////    }
////}


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
