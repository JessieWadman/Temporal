using Temporal.Storage;

namespace Temporal.Notifications
{
    internal class HistoricalChangesSaver : ChangeReceiver
    {
        private readonly IChangeStore store;

        public HistoricalChangesSaver(IChangeStore store)
        {
            this.store = store ?? throw new ArgumentNullException(nameof(store));
        }

        protected override async ValueTask OnChangeApplied(ChangeSet changeSet)
        {
            await store.EnqueueChangesAsync(default, changeSet);
        }
    }
}
