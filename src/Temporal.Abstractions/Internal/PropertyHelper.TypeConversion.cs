using System.ComponentModel;

namespace Temporal.Abstractions.Internal
{
    internal static partial class PropertyHelper
    {
        private static bool IsNullable(Type type) => Nullable.GetUnderlyingType(type) != null;

        public static object? ConvertToType(object source, Type destinationType)
        {
            if (source == null)
                return null; // SetProperty with null uses default(T) when used on a ValueType (.NET 4.0+)

            var sourceType = source.GetType();
            if (sourceType == destinationType || destinationType.IsAssignableFrom(sourceType))
                return source;

            var targetType = IsNullable(destinationType) ? Nullable.GetUnderlyingType(destinationType) : destinationType;

            if (targetType is null)
                return null;

            var converter = TypeDescriptor.GetConverter(targetType);
            if (converter.IsValid(source))
                return converter.ConvertFrom(source);
            else
            {
                var sourceToString = source.ToString();
                var methodParamTypes = new Type[] { typeof(string), destinationType.MakeByRefType() };
                var typeInstance = Activator.CreateInstance(destinationType);
                if (typeInstance is null)
                    return null;

                var methodArgs = new object?[] { sourceToString, typeInstance };
                var tryParse = destinationType.GetMethod("TryParse", methodParamTypes);
                if (tryParse == null)
                    return null;

                if ((bool)tryParse.Invoke(destinationType, methodArgs)!)
                {
                    return methodArgs[1];
                }

                return null;
            }
        }
    }
}
