using Dapper;
using System.Collections.Immutable;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using static Dapper.SqlMapper;

namespace Temporal.Storage
{
    public class SqlChangeStore : IChangeStore
    {
        private readonly string connectionString;
        private readonly string tableName;
        private readonly string schemaName;

        private class ChangeSetDto
        {
            public long ChangeId { get; set; }
            public string TypeName { get; set; }
            public DateTime EffectiveTimestampUtc { get; set; }
            public string Identity { get; set; }
            public string Changes { get; set; }
            public string UserInfo { get; set; }
        }

        public SqlChangeStore(string connectionString, string tableName, string schemaName = "dbo")
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            this.tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            this.schemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
        }

        private SqlConnection CreateConnection()
        {
            return new SqlConnection(connectionString);
        }

        public async Task CreateTable()
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync();

            await connection.ExecuteAsync(
                $@"CREATE TABLE [{schemaName}].[{tableName}]
                (
	                [ChangeId] BIGINT NOT NULL PRIMARY KEY, 
                    [TypeName] NVARCHAR(1024) NOT NULL, 
                    [EffectiveTimestampUtc] DATETIME NOT NULL, 
                    [Identity] NVARCHAR(1024) NOT NULL, 
                    [Changes] NVARCHAR(MAX) NOT NULL, 
                    [UserInfo] NVARCHAR(MAX) NULL
                );");

            await connection.ExecuteAsync($"CREATE INDEX [IX_{tableName}_TypeName_Identity_EffectiveTimestampUtc] ON [{schemaName}].[{tableName}] ([TypeName], [Identity], [EffectiveTimestampUtc]);");
            await connection.ExecuteAsync($"CREATE INDEX [IX_{tableName}_TypeName_EffectiveTimestampUtc] ON [{schemaName}].[{tableName}] ([EffectiveTimestampUtc]);");
        }

        public async ValueTask DeleteChangeSetAsync(long changeId, CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await connection.ExecuteAsync($"DELETE FROM [{schemaName}].[{tableName}] WHERE [ChangeId] = @changeId", new { changeId });
        }

        private static readonly DateTime MinSqlDateTime = new DateTime(1900, 01, 01);

        public async ValueTask EnqueueChangesAsync(CancellationToken cancellationToken, params ChangeSet[] changes)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);

            foreach (var batch in changes.Chunk(10))
            {
                var idx = 0;
                var sb = new StringBuilder();
                var dp = new DynamicParameters();
                foreach (var changeSet in batch)
                {
                    sb.AppendLine($"INSERT INTO [{schemaName}].[{tableName}] ([ChangeId], [EffectiveTimestampUtc], [TypeName], [Identity], [Changes], [UserInfo]) VALUES (@changeSet{idx}, @effectiveTimestampUtc{idx}, @typeName{idx}, @identity{idx}, @changes{idx}, @userInfo{idx});");
                    dp.Add($"changeSet{idx}", changeSet.ChangeId);
                    dp.Add($"effectiveTimestampUtc{idx}",
                        changeSet.EffectiveTimestampUtc < MinSqlDateTime ? MinSqlDateTime : changeSet.EffectiveTimestampUtc);
                    dp.Add($"typeName{idx}", changeSet.TypeName);
                    dp.Add($"identity{idx}", changeSet.Identity);
                    dp.Add($"changes{idx}", JsonSerializer.Serialize(changeSet.Changes));
                    dp.Add($"userInfo{idx}", JsonSerializer.Serialize(changeSet.UserInfo));
                }

                await connection.ExecuteAsync(sb.ToString(), dp);
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(string typeName, string identity, DateTime fromUtc, DateTime toUtc, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * FROM [{schemaName}].[{tableName}] WHERE [TypeName] = @typeName AND [Identity] = @identity AND [EffectiveTimestampUtc] >= @fromUtc AND [EffectiveTimestampUtc] <= @toUtc ORDER BY [EffectiveTimestampUtc], [ChangeId]",
                new { @typeName, @identity, @fromUtc, @toUtc });
            foreach (var change in result)
            {
                yield return new ChangeSet(change.ChangeId, change.TypeName, change.EffectiveTimestampUtc, change.Identity,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.Changes)!,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.UserInfo)!);
            }
        }

        private static readonly DateTime MaxDateTime = new DateTime(2900, 01, 01);

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(DateTime toUtc, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (toUtc > MaxDateTime)
                toUtc = MaxDateTime;
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            ChangeSet[] changes;
            try
            {
                var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * FROM [{schemaName}].[{tableName}] WHERE [EffectiveTimestampUtc] <= @toUtc ORDER BY [EffectiveTimestampUtc], [ChangeId]",
                    new { @toUtc });

                changes = result.Select(change => new ChangeSet(change.ChangeId, change.TypeName, change.EffectiveTimestampUtc, change.Identity,
                        JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.Changes)!,
                        JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.UserInfo)!))
                    .ToArray();
            }
            catch (Exception error)
            {
                throw;
            }

            foreach (var change in changes)
            {
                yield return change;
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(long fromChangeSetId, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * FROM [{schemaName}].[{tableName}] WHERE ChangeId > @fromChangeSetId ORDER BY [EffectiveTimestampUtc], [ChangeId]",
                new { fromChangeSetId });

            foreach (var change in result)
            {
                yield return new ChangeSet(change.ChangeId, change.TypeName, change.EffectiveTimestampUtc, change.Identity,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.Changes)!,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.UserInfo)!);
            }
        }
    }
}
