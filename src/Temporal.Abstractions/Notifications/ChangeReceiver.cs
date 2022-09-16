using System.Threading.Channels;

namespace Temporal.Notifications
{
    public interface IChangeReceiver
    {
        ValueTask PushAsync(ChangeSet changeSet, CancellationToken cancellationToken);
    }

    public abstract class ChangeReceiver : IChangeReceiver, IAsyncDisposable, IDisposable
    {
        internal Channel<ChangeSet> channel = Channel.CreateBounded<ChangeSet>(3);
        private readonly Task completion;

        public ChangeReceiver()
        {
            completion = Task.Factory.StartNew(RunAsync, TaskCreationOptions.LongRunning);
        }

        private async Task RunAsync()
        {
            await foreach (var change in channel.Reader.ReadAllAsync())
            {
                try
                {
                    var call = OnChangeApplied(change);
                    if (!call.IsCompletedSuccessfully)
                        await call;
                }
                catch
                {
                }
            }
        }

        public ValueTask PushAsync(ChangeSet changeSet, CancellationToken cancellationToken)
        {
            if (channel.Writer.TryWrite(changeSet))
                return ValueTask.CompletedTask;

            return channel.Writer.WriteAsync(changeSet, cancellationToken);
        }

        protected abstract ValueTask OnChangeApplied(ChangeSet changeSet);

        public void Dispose() => DisposeAsync().AsTask().Wait();

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            channel.Writer.Complete();
            return new(completion);
        }
    }
}
