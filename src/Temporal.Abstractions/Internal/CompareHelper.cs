using System.Reflection;
using Temporal.Abstractions.Internal;

namespace Temporal.Internal
{
    internal static class CompareHelper
    {
        public static bool Compare(Type propertyType, object? left, object? right)
        {
            if (left is null)
                return right is null;

            if (right is null)
                return left is null;

            return (bool)CompareMethod
                .MakeGenericMethod(propertyType)
                .Invoke(null, new object[] { left, right })!;
        }


        private static readonly MethodInfo CompareMethod = typeof(CompareHelper).GetMethod(nameof(Compare), BindingFlags.NonPublic)!;

        public static bool Compare<T>(T left, T right)
            => EqualityComparer<T>.Default.Equals(left, right);

        public static bool AreEqual(string name, object obj, object value)
        {
            object? node = obj;
            object? nodeValue = obj;
            PropertyInfo? propertyInfo = null;

            if (string.IsNullOrWhiteSpace(name))
                return false;

            foreach (var part in name.Split('.'))
            {
                node = nodeValue;
                (propertyInfo, node, nodeValue) = PropertyHelper.NavigateToProperty(node!, part);
                if (node == null)
                    throw new InvalidOperationException($"Property path [{name}] is invalid.");
                if (propertyInfo == null)
                    throw new InvalidOperationException($"Property path [{name}] is invalid.");
            }

            nodeValue = propertyInfo!.GetValue(node);
            return Compare(nodeValue, value);
        }
    }
}
