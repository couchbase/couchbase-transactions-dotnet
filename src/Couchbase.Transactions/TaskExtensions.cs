using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Couchbase.Transactions
{
    public static class TaskExtensions
    {
        public static ConfiguredValueTaskAwaitable<T> CAF<T>(this ValueTask<T> task)
        {
            return task.ConfigureAwait(false);
        }

        public static ConfiguredTaskAwaitable<T> CAF<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }

        public static ConfiguredTaskAwaitable CAF(this Task task)
        {
            return task.ConfigureAwait(false);
        }
    }
}
