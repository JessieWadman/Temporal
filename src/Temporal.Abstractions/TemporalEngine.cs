using Temporal.Storage;

namespace Temporal
{
    public class TemporalEngine
    {
        public IChangeStore PendingChanges { get; init; }
        public IChangeStore? AppliedChanges { get; init; }
        public IChangeApplier Repository { get; init; }
    }
}
