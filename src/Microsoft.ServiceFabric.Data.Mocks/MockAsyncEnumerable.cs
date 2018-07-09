using System;
using System.Collections.Generic;

namespace Microsoft.ServiceFabric.Data.Mocks
{
	public sealed class MockAsyncEnumerable<T> : IAsyncEnumerable<T>
	{
		private readonly IEnumerable<T> _enumerable;

		public MockAsyncEnumerable(IEnumerable<T> enumerable)
		{
			_enumerable = enumerable ?? throw new ArgumentNullException(nameof(enumerable));
		}

		public IAsyncEnumerator<T> GetAsyncEnumerator()
		{
			return new MockAsyncEnumerator<T>(_enumerable.GetEnumerator());
		}
	}
}
