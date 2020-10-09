using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.Error;
using Couchbase.Transactions.Error.External;
using Couchbase.Transactions.Internal.Test;
using Couchbase.Transactions.Support;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Cleanup
{
    internal class CleanupWorkQueue
    {
        private readonly CancellationTokenSource _forceFlush = new CancellationTokenSource();

        // TODO: This needs to be bounded, with a circuit breaker.
        private readonly BlockingCollection<CleanupRequest> _workQueue = new BlockingCollection<CleanupRequest>();
        private readonly Task _consumer;
        private readonly Cleaner _cleaner;

        public ICleanupTestHooks TestHooks { get; set; } = DefaultCleanupTestHooks.Instance;


        public CleanupWorkQueue(ICluster cluster, TimeSpan? keyValueTimeout, ITypeTranscoder transcoder)
        {
            _cleaner = new Cleaner(cluster, keyValueTimeout, transcoder);
            _consumer = Task.Run(ConsumeWork);
        }

        internal bool TryAddCleanupRequest(CleanupRequest cleanupRequest) => _workQueue.TryAdd(cleanupRequest);

        private async Task ConsumeWork()
        {
            // Initial, naive implementation.
            // Single-threaded consumer that assumes cleanupRequests are in order of transaction expiry already
            foreach (var cleanupRequest in _workQueue.GetConsumingEnumerable())
            {
                var delay = cleanupRequest.WhenReadyToBeProcessed - DateTimeOffset.UtcNow;
                if (delay > TimeSpan.Zero && !_forceFlush.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(delay, _forceFlush.Token).CAF();
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }

                try
                {
                    await _cleaner.ProcessCleanupRequest(cleanupRequest).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // TODO:  need a more intelligent error handling.
                    cleanupRequest.ProcessingErrors.Enqueue(ex);
                    if (cleanupRequest.ProcessingErrors.Count > 100)
                    {
                        // TODO: put it in a dead queue for special handling.
                        // drop the request, for now
                        return;
                    }

                    // retry in 10 seconds plus some jitter
                    cleanupRequest.WhenReadyToBeProcessed = DateTimeOffset.UtcNow.AddSeconds(10).AddMilliseconds(DateTime.UtcNow.Second);
                    TryAddCleanupRequest(cleanupRequest);
                }
            }
        }

        /// <summary>
        /// Call during app shutdown to finish all cleanup request as soon as possible.
        /// </summary>
        /// <returns>A Task representing asynchronous work.</returns>
        internal async Task ForceFlushAsync()
        {
            _forceFlush.Cancel(throwOnFirstException: false);
            _workQueue.CompleteAdding();
            await _consumer;
        }
    }
}
