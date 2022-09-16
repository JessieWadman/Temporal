using Dapper;
using System.Data.SqlClient;

namespace Temporal.Mssql.Tests.Helpers
{
    public class SqlRepository
    {
        private readonly string connectionString;
        private readonly string tableName;

        public SqlRepository(string connectionString, string tableName)
        {
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            this.tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }

        public async Task<Employee> GetByIdentity(string identity)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QueryFirstOrDefaultAsync<Employee>($"SELECT * FROM {tableName} WHERE [Identity] = @identity", new { @identity });
        }

        public async Task<IEnumerable<Employee>> GetAllAsync()
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QueryAsync<Employee>($"SELECT * FROM {tableName}");
        }

        public async Task CreateTable()
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            await connection.ExecuteAsync(
                @$"CREATE TABLE {tableName} (
                        [Identity] NVARCHAR(10),                        
                        [Name] NVARCHAR(100) NOT NULL,
                        [DepartmentId] INT NOT NULL
                    );        
                ");
        }
    }
}
