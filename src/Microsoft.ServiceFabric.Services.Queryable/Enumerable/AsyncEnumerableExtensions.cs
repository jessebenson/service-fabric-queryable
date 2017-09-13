using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public static class AsyncEnumerableExtensions
	{
		public static async Task<int> CountAsync<T>(this IAsyncEnumerable<T> source, CancellationToken token = default(CancellationToken))
		{
			if (source == null) throw new ArgumentNullException(nameof(source));

			int count = 0;
			using (var enumerator = source.GetAsyncEnumerator())
			{
				while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
				{
					checked
					{
						count++;
					}
				}
			}

			return count;
		}

		public static async Task<int> CountAsync<T>(this IAsyncEnumerable<T> source, Func<T, bool> predicate, CancellationToken token = default(CancellationToken))
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));

			int count = 0;
			using (var enumerator = source.GetAsyncEnumerator())
			{
				while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
				{
					checked
					{
						if (predicate(enumerator.Current))
							count++;
					}
				}
			}

			return count;
		}

		public static IAsyncEnumerable<T> WhereAsync<T>(this IAsyncEnumerable<T> source, Func<T, bool> predicate)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));

			return new WhereAsyncEnumerable<T>(source, predicate);
		}

		private sealed class WhereAsyncEnumerable<T> : IAsyncEnumerable<T>
		{
			private readonly IAsyncEnumerable<T> _source;
			private readonly Func<T, bool> _predicate;

			public WhereAsyncEnumerable(IAsyncEnumerable<T> source, Func<T, bool> predicate)
			{
				_source = source;
				_predicate = predicate;
			}

			public IAsyncEnumerator<T> GetAsyncEnumerator()
			{
				return new WhereAsyncEnumerator(_source.GetAsyncEnumerator(), _predicate);
			}

			private sealed class WhereAsyncEnumerator : IAsyncEnumerator<T>
			{
				private readonly IAsyncEnumerator<T> _source;
				private readonly Func<T, bool> _predicate;

				public WhereAsyncEnumerator(IAsyncEnumerator<T> source, Func<T, bool> predicate)
				{
					_source = source;
					_predicate = predicate;
				}

				public T Current => _source.Current;

				public void Dispose()
				{
					_source.Dispose();
				}

				public async Task<bool> MoveNextAsync(CancellationToken cancellationToken)
				{
					while (await _source.MoveNextAsync(cancellationToken).ConfigureAwait(false))
					{
						if (_predicate.Invoke(Current))
							return true;
					}

					return false;
				}

				public void Reset()
				{
					throw new NotImplementedException();
				}
			}
		}
	}
}
