using System.Runtime.CompilerServices;
using Temporal.Storage;

namespace Temporal.InMemory
{
    public class InMemoryChangeStore : IChangeStore
    {
        private readonly Dictionary<long, ChangeSet> changes = new();

        public ValueTask DeleteChangeSetAsync(long changeSetId, CancellationToken cancellationToken)
        {
            if (changes.ContainsKey(changeSetId))
                changes.Remove(changeSetId);
            return ValueTask.CompletedTask;
        }

        public ValueTask EnqueueChangesAsync(CancellationToken cancellationToken, params ChangeSet[] changes)
        {
            foreach (var change in changes)
                this.changes.Add(change.ChangeId, change);
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(
            string typeName,
            string identity,
            DateTime fromUtc,
            DateTime toUtc,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var query = changes.Values
                .Where(c => c.TypeName == typeName && c.Identity == identity && c.EffectiveTimestampUtc >= fromUtc && c.EffectiveTimestampUtc <= toUtc)
                .OrderBy(c => c.EffectiveTimestampUtc)
                .ThenBy(c => c.ChangeId)
                .ToArray();

            foreach (var change in query)
            {
                yield return change;
                await Task.Yield();
                if (cancellationToken.IsCancellationRequested)
                    yield break;
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(
            DateTime fromUtc,
            DateTime toUtc,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var query = changes.Values
                .Where(c => c.EffectiveTimestampUtc >= fromUtc && c.EffectiveTimestampUtc <= toUtc)
                .OrderBy(c => c.EffectiveTimestampUtc)
                .ThenBy(c => c.ChangeId)
                .ToArray();

            foreach (var change in query)
            {
                yield return change;
                await Task.Yield();
                if (cancellationToken.IsCancellationRequested)
                    yield break;
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(long fromChangeSetId, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var query = changes.Values
                .Where(c => c.ChangeId > fromChangeSetId)
                .OrderBy(c => c.EffectiveTimestampUtc)
                .ThenBy(c => c.ChangeId)
                .ToArray();

            foreach (var change in query)
            {
                yield return change;
                await Task.Yield();
                if (cancellationToken.IsCancellationRequested)
                    yield break;
            }
        }
    }
}
