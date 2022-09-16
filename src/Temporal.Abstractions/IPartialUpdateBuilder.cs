using System.Linq.Expressions;

namespace Temporal;

public interface IPartialUpdateBuilder<T>
{
    IPartialUpdateBuilder<T> Set<P>(Expression<Func<T, P>> property, P value);
    void Clear<P>(Expression<Func<T, P>> property) => Set(property, default!);
}
