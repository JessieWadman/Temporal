namespace Temporal.Notifications
{
    internal class PendingChangesApplier : ChangeReceiver
    {
        private readonly IEntityChangeApplier changeApplier;

        public PendingChangesApplier(IEntityChangeApplier changeApplier)
        {
            this.changeApplier = changeApplier ?? throw new ArgumentNullException(nameof(changeApplier));
        }

        protected override async ValueTask OnChangeApplied(ChangeSet changeSet)
        {
            await changeApplier.ApplyAsync(changeSet, default);
        }
    }
}
