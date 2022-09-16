using Temporal.Internal;
using Temporal.Storage;

namespace Temporal.Notifications
{
    public record ChangePollerOptions
    {
        public TimeSpan PollingFrequency { get; set; } = TimeSpan.FromSeconds(10);
    }

    public class ChangePoller : IAsyncDisposable, IDisposable
    {
        protected readonly IChangeStore Store;
        private readonly IChangeReceiver[] changeReceivers;
        private readonly ISystemClock systemClock;
        private readonly ChangePollerOptions options;
        private readonly Task completion;
        private readonly CancellationTokenSource stoppingTokenSource = new();

        public ChangePoller(
            IChangeStore store,
            IChangeReceiver[] changeReceivers,
            ISystemClock systemClock,
            ChangePollerOptions options)
        {
            Store = store ?? throw new ArgumentNullException(nameof(store));
            this.changeReceivers = changeReceivers ?? throw new ArgumentNullException(nameof(changeReceivers));
            this.systemClock = systemClock ?? throw new ArgumentNullException(nameof(systemClock));
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            completion = Task.Factory.StartNew(() => RunAsync(stoppingTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        private async Task<bool> RetryForever(Func<Task> action, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await action();
                    return true;
                }
                catch (TaskCanceledException)
                {
                    return false;
                }
                catch
                {
                    try
                    {
                        await Task.Delay(1000, cancellationToken);
                    }
                    catch (TaskCanceledException)
                    {
                        return false;
                    }
                }
            }
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await foreach (var change in Store.GetChangeSetsAsync(DateTime.MinValue, systemClock.UtcNow, cancellationToken))
                {
                    foreach (var receiver in changeReceivers)
                        await RetryForever(async () => await receiver.PushAsync(change, cancellationToken), cancellationToken);

                    await RetryForever(async () => await Store.DeleteChangeSetAsync(change.ChangeId, cancellationToken), cancellationToken);
                }

                await Task.Delay(options.PollingFrequency, cancellationToken);
            }
        }

        public async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            using (stoppingTokenSource)
            {
                stoppingTokenSource.Cancel();
                await completion;
            }
        }

        public void Dispose() => DisposeAsync().AsTask().Wait();
    }
}
