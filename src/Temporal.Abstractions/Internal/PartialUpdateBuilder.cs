using System.Linq.Expressions;
using System.Text.Json;
using Temporal.Abstractions.Internal;

namespace Temporal.Internal;

public class PartialUpdateBuilder<T> : IPartialUpdateBuilder<T>
{
    private readonly Dictionary<string, string> changes;

    public PartialUpdateBuilder(Dictionary<string, string> changes)
    {
        this.changes = changes ?? throw new ArgumentNullException(nameof(changes));
    }

    public IPartialUpdateBuilder<T> Set<P>(Expression<Func<T, P>> property, P value)
    {
        changes.Add(PropertyHelper.ToDotNotation(property).PropertyPath, JsonSerializer.Serialize(value));
        return this;
    }
}
