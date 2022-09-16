using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Temporal.Internal;

namespace Temporal.Abstractions.Internal
{
    internal static partial class PropertyHelper
    {
        class Visitor : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression memberExpression)
            {
                // Recurse down to see if we can simplify...
                var expression = Visit(memberExpression.Expression);

                // If we've ended up with a constant, and it's a property or a field,
                // we can simplify ourselves to a constant
                if (expression is ConstantExpression constantExpression)
                {
                    var container = constantExpression.Value;
                    var member = memberExpression.Member;
                    if (member is FieldInfo fi)
                    {
                        var value = fi.GetValue(container);
                        return Expression.Constant(value, fi.FieldType);
                    }
                    if (member is PropertyInfo propertyInfo)
                    {
                        var value = propertyInfo.GetValue(container, null);
                        return Expression.Constant(value);
                    }
                }
                return base.VisitMember(memberExpression);
            }
        }

        public static (string PropertyPath, Type MemberType) ToDotNotation<TSource, TProp>(Expression<Func<TSource, TProp>> pathExpression)
        {
            pathExpression = (Expression<Func<TSource, TProp>>)new Visitor().Visit(pathExpression);

            var str = pathExpression.Body.ToString();
            str = str.Substring(str.IndexOf(".") + 1);
            str = str.Replace(".get_Item(\"", "[\"");
            str = str.Replace("\")", "\"]");
            str = str.Replace(".get_Item(", "[");
            str = str.Replace(")", "]");

            return (str, pathExpression.ReturnType);
        }

        internal static (PropertyInfo? PropertyInfo, object? Object, object? Value) NavigateToProperty(object obj, string path)
        {
            if (obj == null)
                return (null, null, null);

            var temp = path;
            if (temp.Contains('['))
                temp = temp[..temp.IndexOf("[")];

            var property = obj.GetType().GetProperty(temp, BindingFlags.Public | BindingFlags.Instance);
            if (property == null)
                return (null, null, null);

            var value = property.GetValue(obj);

            if (path.Contains('['))
            {
                temp = path[(path.IndexOf("[") + 1)..];
                temp = temp[..temp.IndexOf("]")];
                object indexValue;
                if (temp.StartsWith("\""))
                {
                    temp = temp[1..];
                    if (temp.EndsWith("\""))
                        temp = temp[0..^1];
                    indexValue = temp;
                }
                else
                {
                    _ = long.Parse(temp.ToString());
                    indexValue = Convert.ChangeType(temp, property.PropertyType.GetGenericArguments().First());
                }

                if (value is null)
                    return (null, null, null);
                var getOrAddResult = GetOrAdd(value, indexValue);
                if (getOrAddResult.Dictionary != value)
                    property.SetValue(obj, getOrAddResult.Dictionary);
                value = getOrAddResult.Value;
            }

            return (property, obj, value);
        }

        public static bool SetPropertyValue(string propertyPath, object obj, object? value)
        {
            object? node = obj;
            object? nodeValue = obj;
            PropertyInfo? propertyInfo = null;

            if (string.IsNullOrWhiteSpace(propertyPath))
                return false;

            foreach (var part in propertyPath.Split('.'))
            {
                node = nodeValue;
                (propertyInfo, node, nodeValue) = NavigateToProperty(node!, part);
                if (node is null)
                    throw new InvalidOperationException($"Property path [{propertyPath}] is invalid.");
                if (propertyInfo is null)
                    throw new InvalidOperationException($"Property path [{propertyPath}] is invalid.");
            }

            nodeValue = propertyInfo!.GetValue(node);
            if (value is string str)
            {
                value = JsonSerializer.Deserialize(str, propertyInfo.PropertyType);
            }
            propertyInfo.SetValue(node, value);

            return !CompareHelper.Compare(nodeValue, value);
        }
    }
}
