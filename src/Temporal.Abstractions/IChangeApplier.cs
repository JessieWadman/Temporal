namespace Temporal;

public interface IChangeApplier
{
    ValueTask ApplyAsync(ChangeSet changeSet, CancellationToken cancellationToken);
}