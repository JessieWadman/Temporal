namespace Temporal.Internal
{
    internal static class TypeNameHelper
    {
        public static string GetTypeName<T>() => GetTypeName(typeof(T));
        public static string GetTypeName(Type type)
        {
            return $"{type.FullName}, {type.Assembly.GetName().Name}";
        }

        public static Type GetType(string typeName)
            => Type.GetType(typeName) ?? throw new TypeLoadException($"Could not load type {typeName}");
    }
}
