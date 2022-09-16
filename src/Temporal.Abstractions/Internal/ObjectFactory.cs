using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace Temporal.Internal
{
    internal static class ObjectFactory
    {
        private static Func<T> Factory<T>()
            => Expression.Lambda<Func<T>>(
                Expression.New(typeof(T).GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, null, Type.EmptyTypes, null)
                    ?? throw new InvalidOperationException($"No parameterless constructor for type {typeof(T).Name}")
            )
        ).Compile();

        static readonly MethodInfo objectFactoryMethod = typeof(ObjectFactory).GetMethod(nameof(Factory), BindingFlags.Static | BindingFlags.NonPublic)!;
        static readonly object[] emptyArgs = Array.Empty<object>();
        static readonly Type stringType = typeof(string);

        private static readonly ConcurrentDictionary<Type, Func<object>> factoryCache = new();

        private static object CreateNewObj(Type type)
        {
            return factoryCache.GetOrAdd(type, t => CreateObjectFactory(t))();
        }

        static Func<object> CreateObjectFactory(Type type)
            => (Func<object>)objectFactoryMethod.MakeGenericMethod(type)!.Invoke(null, emptyArgs)!;

        private static object EmptyImmutable(Type type)
        {
            return type.GetField("Empty", BindingFlags.Public | BindingFlags.Static)!
                .GetValue(null)!;
        }

        public static object InitializeObject(Type type)
        {
            if ((type.Namespace ?? string.Empty).Equals("System.Collections.Immutable", StringComparison.Ordinal))
                return EmptyImmutable(type);

            var temp = CreateNewObj(type);

            if (type.Name.StartsWith("Dictionary`"))
                return temp;

            // var temp = RuntimeHelpers.GetUninitializedObject(type);
            foreach (var field in type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance))
            {
                if (!field.FieldType.IsValueType &&
                    !field.FieldType.IsPrimitive &&
                    !field.FieldType.IsEnum &&
                    field.FieldType != stringType)
                    field.SetValue(temp, InitializeObject(field.FieldType));
            }
            return temp;
        }
    }
}
