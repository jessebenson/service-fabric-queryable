using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Data.Mocks
{
	public sealed class MockAsyncEnumerator<T> : IAsyncEnumerator<T>
	{
		private readonly IEnumerator<T> _enumerator;

		public MockAsyncEnumerator(IEnumerator<T> enumerator)
		{
			_enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
		}

		public T Current => _enumerator.Current;

		public void Dispose()
		{
			_enumerator.Dispose();
		}

		public Task<bool> MoveNextAsync(CancellationToken cancellationToken)
		{
			return Task.FromResult(_enumerator.MoveNext());
		}

		public void Reset()
		{
			_enumerator.Reset();
		}
	}
}
