namespace Temporal;

public interface IEntityChangeApplier
{
    ValueTask ApplyAsync(ChangeSet changeSet, CancellationToken cancellationToken);
}