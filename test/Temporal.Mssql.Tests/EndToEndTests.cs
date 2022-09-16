using System.Collections.Immutable;
using Temporal.Internal;
using Temporal.Mssql.Tests.Helpers;
using Temporal.Notifications;
using Temporal.Storage;

namespace Temporal.Mssql.Tests
{
    public class EndToEndTests
    {
        [Fact]
        public async Task ShouldWork()
        {
            var connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=TemporalTests;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

            var runId = Guid64.NextId();

            var employees = new SqlRepository(connectionString, $"Employees_{runId}");
            var pendingChanges = new SqlChangeStore(connectionString, $"PendingChanges_{runId}");
            var appliedChanges = new SqlChangeStore(connectionString, $"History_{runId}");
            var changeApplier = new SqlChangeApplier(connectionString, $"Employees_{runId}", nameof(Employee.Identity));
            var systemClock = new MockSystemClock();

            await pendingChanges.CreateTable();
            await appliedChanges.CreateTable();
            await employees.CreateTable();

            await using var pengingChangesApplier = new PendingChangesApplier(changeApplier);
            await using var historicalEventsSaver = new HistoricalChangesSaver(appliedChanges);
            await using var poller = new ChangePoller(
                pendingChanges,
                new IChangeReceiver[] { pengingChangesApplier, historicalEventsSaver },
                systemClock,
                new ChangePollerOptions { PollingFrequency = TimeSpan.FromMilliseconds(10) });

            var employee = new Employee
            {
                Identity = "1",
                Name = "Test Person",
                DepartmentId = 2
            };

            var storedEmployee = await employees.GetByIdentity("1");
            Assert.Null(storedEmployee);

            await pendingChanges.EnqueueChangesAsync(default,
                ChangeSet.CreateSnapshot(
                    DateTime.MinValue,
                    "1",
                    employee,
                    ImmutableDictionary<string, string>.Empty.Add("ModifiedBy", "User1")
                )
            );

            for (var i = 0; i < 5; i++)
            {
                await Task.Delay(10);
                storedEmployee = await employees.GetByIdentity("1");
                if (storedEmployee is not null)
                    break;
            }

            Assert.NotNull(storedEmployee);
            Assert.Equal("1", storedEmployee!.Identity);
            Assert.Equal("Test Person", storedEmployee.Name);
            Assert.Equal(2, storedEmployee.DepartmentId);

            var shouldApplyAt = DateTime.UtcNow.AddMilliseconds(200);

            await pendingChanges.EnqueueChangesAsync(default,
                ChangeSet.CreatePartialUpdate<Employee>(
                    shouldApplyAt,
                    "1",
                    builder => builder.Set(e => e.DepartmentId, 9),
                    ImmutableDictionary<string, string>.Empty.Add("ModifiedBy", "User1")
                )
            );

            await Task.Delay(50);
            storedEmployee = await employees.GetByIdentity("1");
            Assert.NotNull(storedEmployee);
            Assert.Equal(2, storedEmployee!.DepartmentId);

            systemClock.NowWithOffset = shouldApplyAt.AddSeconds(1);

            await Task.Delay(50);
            storedEmployee = await employees.GetByIdentity("1");
            Assert.NotNull(storedEmployee);
            Assert.Equal(9, storedEmployee!.DepartmentId);

            List<ChangeSet> allAppliedChanges = new();
            await foreach (var appliedChange in appliedChanges.GetChangeSetsAsync(0, default))
                allAppliedChanges.Add(appliedChange);
            Assert.Equal(2, allAppliedChanges.Count);
        }
    }
}