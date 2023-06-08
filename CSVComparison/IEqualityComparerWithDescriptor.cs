using System.Collections;
using System.Collections.Generic;

namespace CSVComparison
{
	public interface IEqualityComparerWithDescriptor<in T> : IEqualityComparer<T>, IEqualityComparerWithDescriptor
	{
		(bool Equal, string Description) EqualsWithDescription(T x, T y);
	}
	public interface IEqualityComparerWithDescriptor : IEqualityComparer
	{
		(bool Equal, string Description) EqualsWithDescription(object x, object y);
	}
}