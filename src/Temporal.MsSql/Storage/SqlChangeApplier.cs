using Dapper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Temporal.Abstractions.Internal;
using Temporal.Internal;

namespace Temporal.Storage
{
    public class SqlChangeApplier : IChangeApplier
    {
        private readonly string connectionString;
        private readonly string tableName;
        private readonly string identityColumnName;

        public SqlChangeApplier(string connectionString, string tableName, string identityColumnName)
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            this.tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            this.identityColumnName = identityColumnName ?? throw new ArgumentNullException(nameof(identityColumnName));
        }

        private readonly ConcurrentDictionary<string, Type> typeNames = new();
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, PropertyInfo>> properties = new();

        public async ValueTask ApplyAsync(ChangeSet changeSet, CancellationToken cancellationToken)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var type = typeNames.GetOrAdd(changeSet.TypeName, t => TypeNameHelper.GetType(t));

            var sb = new StringBuilder();
            sb.AppendLine($"UPDATE {tableName} SET");
            var args = new DynamicParameters();
            var first = true;
            foreach (var change in changeSet.Changes)
            {
                if (!first)
                    sb.AppendLine(",");
                else
                    first = false;

                sb.Append($"  [{change.Key}] = @{change.Key}");

                var propertyInfo = properties.GetOrAdd(type, _ => new())
                                             .GetOrAdd(change.Key, p => type.GetProperty(p, BindingFlags.Public | BindingFlags.Instance)!);
                args.Add(change.Key, JsonSerializer.Deserialize(change.Value, propertyInfo.PropertyType));
            }
            sb.AppendLine();
            sb.AppendLine($"WHERE [{identityColumnName}] = @identity;");
            args.Add("@identity", changeSet.Identity);

            sb.AppendLine("IF @@ROWCOUNT = 0");
            sb.AppendLine("BEGIN");
            sb.Append($"  INSERT INTO {tableName} (");
            sb.Append(string.Join(", ", changeSet.Changes.Keys.Select(k => $"[{k}]")));
            sb.Append(") VALUES (");
            sb.Append(string.Join(", ", changeSet.Changes.Keys.Select(k => $"@{k}")));
            sb.AppendLine(");");
            sb.AppendLine("END");

            await connection.ExecuteAsync(sb.ToString(), args);
        }
    }
}
