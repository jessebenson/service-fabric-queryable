using Microsoft.ServiceFabric.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal sealed class DefaultAsyncEnumerable<T> : IAsyncEnumerable<T>
	{
		private readonly IEnumerable<T> _source;

		public DefaultAsyncEnumerable(IEnumerable<T> source)
		{
			_source = source;
		}

		public IAsyncEnumerator<T> GetAsyncEnumerator()
		{
			return new DefaultAsyncEnumerator<T>(_source.GetEnumerator());
		}
	}

	internal sealed class DefaultAsyncEnumerator<T> : IAsyncEnumerator<T>
	{
		private readonly IEnumerator<T> _source;

		public DefaultAsyncEnumerator(IEnumerator<T> source)
		{
			_source = source;
		}

		public T Current => _source.Current;

		public void Dispose()
		{
			_source.Dispose();
		}

		public Task<bool> MoveNextAsync(CancellationToken cancellationToken)
		{
			return Task.FromResult(_source.MoveNext());
		}

		public void Reset()
		{
			_source.Reset();
		}
	}
}
