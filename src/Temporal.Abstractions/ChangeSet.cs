using System.Collections.Immutable;
using System.Text.Json;
using Temporal.Abstractions.Internal;
using Temporal.Internal;

namespace Temporal;

public record ChangeSet(long ChangeId, string TypeName, DateTime EffectiveTimestampUtc, string Identity, ImmutableDictionary<string, string> Changes, ImmutableDictionary<string, string> UserInfo)
{
    public static ChangeSet CreateSnapshot<T>(DateTime effectiveTimestampUtc, string identity, T snapshot, ImmutableDictionary<string, string>? userInfo = null)
    {
        using var ms = new MemoryStream();
        using var doc = JsonSerializer.SerializeToDocument(snapshot);
        var changes = new Dictionary<string, string>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            changes[prop.Name] = prop.Value.GetRawText();
        }
        return new(Guid64.NextId(), TypeNameHelper.GetTypeName<T>(), effectiveTimestampUtc, identity, changes.ToImmutableDictionary(), userInfo ?? ImmutableDictionary<string, string>.Empty);
    }

    public static ChangeSet CreatePartialUpdate<T>(DateTime effectiveTimestampUtc, string identity, Action<IPartialUpdateBuilder<T>> builder, ImmutableDictionary<string, string>? userInfo = null)
    {
        var changes = new Dictionary<string, string>();
        builder(new PartialUpdateBuilder<T>(changes));
        return new(Guid64.NextId(), TypeNameHelper.GetTypeName<T>(), effectiveTimestampUtc, identity, changes.ToImmutableDictionary(), userInfo ?? ImmutableDictionary<string, string>.Empty);
    }
}

public static class ChangeSetExtensions
{
    public static void Apply(this ChangeSet changeSet, object instance)
    {
        if (instance is null)
            throw new ArgumentNullException(nameof(instance));

        if (TypeNameHelper.GetTypeName(instance.GetType()) != changeSet.TypeName)
            throw new InvalidOperationException("The type name did not match!");
        foreach (var c in changeSet.Changes)
            PropertyHelper.SetPropertyValue(c.Key, instance!, c.Value);
    }
}
