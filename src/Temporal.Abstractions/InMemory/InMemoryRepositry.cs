using System.Collections.Concurrent;
using Temporal.Internal;

namespace Temporal.InMemory
{
    public class InMemoryRepositry : IChangeApplier
    {
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, object>> typeToIdentityToEntities = new();

        public ValueTask ApplyAsync(ChangeSet changeSet, CancellationToken cancellationToken)
        {
            var identityToEntities = typeToIdentityToEntities.GetOrAdd(changeSet.TypeName, _ => new());

            var entity = identityToEntities.GetOrAdd(changeSet.Identity, i => CreateObj(changeSet.TypeName));
            changeSet.Apply(entity);

            return ValueTask.CompletedTask;
        }

        private object CreateObj(string typeName)
        {
            var type = TypeNameHelper.GetType(typeName);
            return ObjectFactory.InitializeObject(type);
        }

        public ValueTask<T?> GetCurrentAsync<T>(string identity, CancellationToken cancellationToken)
        {
            var identityToEntities = typeToIdentityToEntities.GetOrAdd(TypeNameHelper.GetTypeName<T>(), _ => new());

            if (!identityToEntities.TryGetValue(identity, out var entity))
                return new(default(T));
            return new((T)entity);
        }
    }
}
