using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Couchbase.Transactions
{
    public static class TaskExtensions
    {
        public static ConfiguredTaskAwaitable CAF(this Task awaitable) => awaitable.ConfigureAwait(false);
        public static ConfiguredTaskAwaitable<T> CAF<T>(this Task<T> awaitable) => awaitable.ConfigureAwait(false);
        public static ConfiguredValueTaskAwaitable CAF(this ValueTask awaitable) => awaitable.ConfigureAwait(false);
        public static ConfiguredValueTaskAwaitable<T> CAF<T>(this ValueTask<T> awaitable) => awaitable.ConfigureAwait(false);
    }
}
