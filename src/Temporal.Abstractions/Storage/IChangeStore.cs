using Temporal.Internal;

namespace Temporal.Storage;

public interface IChangeStore
{
    IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(string typeName, string identity, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken);
    IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(DateTime toUtc, CancellationToken cancellationToken);
    IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(long fromChangeSetId, CancellationToken cancellationToken);
    ValueTask DeleteChangeSetAsync(long changeId, CancellationToken cancellationToken);
    ValueTask EnqueueChangesAsync(CancellationToken cancellationToken, params ChangeSet[] changes);

    IAsyncEnumerable<ChangeSet> GetChangeSetsAsync<T>(string identity, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken)
        => GetChangeSetsAsync(TypeNameHelper.GetTypeName<T>(), identity, fromUtc, toUtc, cancellationToken);
}