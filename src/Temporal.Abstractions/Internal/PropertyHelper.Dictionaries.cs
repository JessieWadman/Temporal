using System.Collections.Immutable;
using System.Reflection;
using Temporal.Internal;

namespace Temporal.Abstractions.Internal
{
    internal static partial class PropertyHelper
    {
        private static readonly MethodInfo GetOrAddGenericMethod = typeof(PropertyHelper).GetMethod(nameof(GetOrAddGeneric), BindingFlags.Static | BindingFlags.NonPublic)!;
        private static readonly MethodInfo GetOrAddImmutableGenericMethod = typeof(PropertyHelper).GetMethod(nameof(GetOrAddImmutableGeneric), BindingFlags.Static | BindingFlags.NonPublic)!;

        private static GetOrAddResult GetOrAdd(object dict, object key)
        {
            if (dict.GetType().Namespace == "System.Collections.Immutable")
                return (GetOrAddResult)GetOrAddImmutableGenericMethod
                    .MakeGenericMethod(dict.GetType().GenericTypeArguments)
                    .Invoke(null, new object[] { dict, key })!;
            else
                return (GetOrAddResult)GetOrAddGenericMethod
                    .MakeGenericMethod(dict.GetType().GenericTypeArguments)
                    .Invoke(null, new object[] { dict, key })!;
        }

        private static GetOrAddResult GetOrAddGeneric<TKey, TValue>(IDictionary<TKey, TValue> dict, TKey key) where TValue : class
        {
            if (!dict.TryGetValue(key, out var value))
                dict[key] = (value = (TValue)ObjectFactory.InitializeObject(typeof(TValue)));
            return (value, dict);
        }

        private static GetOrAddResult GetOrAddImmutableGeneric<TKey, TValue>(ImmutableDictionary<TKey, TValue> dict, TKey key) where TValue : class
        {
            if (!dict.TryGetValue(key, out var value))
            {
                value = (TValue)ObjectFactory.InitializeObject(typeof(TValue));
                dict = dict.Add(key, value);
            }
            return (value, dict);
        }

        private class GetOrAddResult
        {
            public object Value;
            public object Dictionary;

            public static implicit operator GetOrAddResult((object value, object dict) other)
                => new GetOrAddResult { Value = other.value, Dictionary = other.dict };
        }
    }
}
