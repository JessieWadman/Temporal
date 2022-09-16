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

        private class ChangeSetDto
        {
            public long ChangeId { get; set; }
            public string TypeName { get; set; }
            public DateTime EffectiveTimestampUtc { get; set; }
            public string Identity { get; set; }
            public string Changes { get; set; }
            public string UserInfo { get; set; }
        }

        public SqlChangeStore(string connectionString, string tableName)
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            this.tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }

        private SqlConnection CreateConnection()
        {
            return new SqlConnection(connectionString);
        }

        public async ValueTask DeleteChangeSetAsync(long changeSetId, CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await connection.ExecuteAsync($"DELETE FROM {tableName} WHERE ChangeSetId = @changeSetId", new { @changeSetId });
        }

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
                    sb.AppendLine($"INSERT INTO {tableName} (ChangeSetId, TypeName, Identity, Changes, UserInfo) VALUES (@changeSet{idx}, @typeName{idx}, @identity{idx}, @changes{idx}, @userInfo{idx});");
                    dp.Add($"changeSet{idx}", changeSet.ChangeId);
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
            var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * {tableName} WHERE TypeName = @typeName AND Identity = @identity AND EffectiveTimestampUtc >= @fromUtc AND EffectiveTimestampUtc <= @toUtc",
                new { @typeName, @identity, @fromUtc, @toUtc });
            foreach (var change in result)
            {
                yield return new ChangeSet(change.ChangeId, change.TypeName, change.EffectiveTimestampUtc, change.Identity,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.Changes)!,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.UserInfo)!);
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(DateTime fromUtc, DateTime toUtc, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * {tableName} WHERE EffectiveTimestampUtc >= @fromUtc AND EffectiveTimestampUtc <= @toUtc",
                new { @fromUtc, @toUtc });
            foreach (var change in result)
            {
                yield return new ChangeSet(change.ChangeId, change.TypeName, change.EffectiveTimestampUtc, change.Identity,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.Changes)!,
                    JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(change.UserInfo)!);
            }
        }

        public async IAsyncEnumerable<ChangeSet> GetChangeSetsAsync(long fromChangeSetId, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            var result = await connection.QueryAsync<ChangeSetDto>($"SELECT * {tableName} WHERE ChangeId > @fromChangeSetId",
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
