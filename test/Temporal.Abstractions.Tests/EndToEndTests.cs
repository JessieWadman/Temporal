using System.Collections.Immutable;
using Temporal.InMemory;
using Temporal.Internal;
using Temporal.Notifications;

namespace Temporal.Abstractions.Tests
{
    public class EndToEndTests
    {
        public class Employee
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int DepartmentId { get; set; }
        }

        [Fact]
        public async Task ShouldWork()
        {
            var entities = new InMemoryRepositry();
            var pendingChanges = new InMemoryChangeStore();
            var appliedChanges = new InMemoryChangeStore();
            var systemClock = new MockSystemClock();

            await using var pengingChangesApplier = new PendingChangesApplier(entities);
            await using var historicalEventsSaver = new HistoricalChangesSaver(appliedChanges);
            await using var poller = new ChangePoller(
                pendingChanges,
                new IChangeReceiver[] { pengingChangesApplier, historicalEventsSaver },
                systemClock,
                new ChangePollerOptions { PollingFrequency = TimeSpan.FromMilliseconds(10) });

            var employee = new Employee
            {
                Id = 1,
                Name = "Test Person",
                DepartmentId = 2
            };

            var storedEmployee = await entities.GetCurrentAsync<Employee>("1", default);
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
                storedEmployee = await entities.GetCurrentAsync<Employee>("1", default);
                if (storedEmployee is not null)
                    break;
            }

            Assert.NotNull(storedEmployee);
            Assert.Equal(1, storedEmployee!.Id);
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
            storedEmployee = await entities.GetCurrentAsync<Employee>("1", default);
            Assert.NotNull(storedEmployee);
            Assert.Equal(2, storedEmployee!.DepartmentId);

            systemClock.NowWithOffset = shouldApplyAt.AddSeconds(1);

            await Task.Delay(50);
            storedEmployee = await entities.GetCurrentAsync<Employee>("1", default);
            Assert.NotNull(storedEmployee);
            Assert.Equal(9, storedEmployee!.DepartmentId);

            List<ChangeSet> allAppliedChanges = new();
            await foreach (var appliedChange in appliedChanges.GetChangeSetsAsync(0, default))
                allAppliedChanges.Add(appliedChange);
            Assert.Equal(2, allAppliedChanges.Count);
        }
    }
}
