using Temporal.Internal;

namespace Temporal.Abstractions.Tests
{
    public class ChangeSetTests
    {
        public class Employee
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int DepartmentId { get; set; }
        }


        [Fact]
        public void FullUpdateShouldWork()
        {
            var employee = new Employee
            {
                Id = 1,
                Name = "test",
                DepartmentId = 4
            };

            var effectiveDate = DateTime.UtcNow;
            var changeSet = ChangeSet.CreateSnapshot(effectiveDate, "1", employee);
            Assert.NotNull(changeSet);
            Assert.Equal("1", changeSet.Identity);
            Assert.Equal(TypeNameHelper.GetTypeName<Employee>(), changeSet.TypeName);
            Assert.Equal(effectiveDate, changeSet.EffectiveTimestampUtc);

            Assert.Equal(3, changeSet.Changes.Count);

            var newEmployee = new Employee();
            changeSet.Apply(newEmployee);

            Assert.Equal(employee.Id, newEmployee.Id);
            Assert.Equal(employee.Name, newEmployee.Name);
            Assert.Equal(employee.DepartmentId, newEmployee.DepartmentId);
        }

        [Fact]
        public void PartialUpdateShouldWork()
        {
            var effectiveDate = DateTime.UtcNow;
            var changeSet = ChangeSet.CreatePartialUpdate<Employee>(effectiveDate, "1", builder => builder
                .Set(e => e.DepartmentId, 6)
                .Set(e => e.Name, "Hello world")
            );
            Assert.NotNull(changeSet);
            Assert.Equal("1", changeSet.Identity);
            Assert.Equal(TypeNameHelper.GetTypeName<Employee>(), changeSet.TypeName);
            Assert.Equal(effectiveDate, changeSet.EffectiveTimestampUtc);

            Assert.Equal(2, changeSet.Changes.Count);

            var newEmployee = new Employee { Id = 4, Name = "No set", DepartmentId = -1 };
            changeSet.Apply(newEmployee);

            Assert.Equal(4, newEmployee.Id);
            Assert.Equal("Hello world", newEmployee.Name);
            Assert.Equal(6, newEmployee.DepartmentId);

            
        }
    }
}