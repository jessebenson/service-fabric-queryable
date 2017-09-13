using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public static class AsyncEnumerable
	{
		public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IEnumerable<T> source)
		{
			return new DefaultAsyncEnumerable<T>(source);
		}

		public static Task<bool> ContainsAsync<T>(this IAsyncEnumerable<T> source, T value, CancellationToken token = default(CancellationToken))
		{
			return ContainsAsync(source, value, null, token);
		}

		public static async Task<bool> ContainsAsync<T>(this IAsyncEnumerable<T> source, T value, IEqualityComparer<T> comparer, CancellationToken token = default(CancellationToken))
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (comparer == null) comparer = EqualityComparer<T>.Default;

			using (var enumerator = source.GetAsyncEnumerator())
			{
				while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
				{
					if (comparer.Equals(enumerator.Current, value))
						return true;
				}
			}

			return false;
		}

		public static Task<int> CountAsync<T>(this IAsyncEnumerable<T> source, CancellationToken token = default(CancellationToken))
		{
			return CountAsync(source, _ => true, token);
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

		public static IAsyncEnumerable<T> EmptyAsync<T>()
		{
			return Enumerable.Empty<T>().AsAsyncEnumerable();
		}

		public static Task<bool> SequenceEqualAsync<T>(this IAsyncEnumerable<T> first, IAsyncEnumerable<T> second, CancellationToken token = default(CancellationToken))
		{
			return SequenceEqualAsync(first, second, null, token);
		}

		public static async Task<bool> SequenceEqualAsync<T>(this IAsyncEnumerable<T> first, IAsyncEnumerable<T> second, IEqualityComparer<T> comparer, CancellationToken token = default(CancellationToken))
		{
			if (first == null) throw new ArgumentNullException(nameof(first));
			if (second == null) throw new ArgumentNullException(nameof(second));
			if (comparer == null) comparer = EqualityComparer<T>.Default;

			using (var e1 = first.GetAsyncEnumerator())
			using (var e2 = second.GetAsyncEnumerator())
			{
				while (await e1.MoveNextAsync(token).ConfigureAwait(false))
				{
					if (!(await e2.MoveNextAsync(token).ConfigureAwait(false) && comparer.Equals(e1.Current, e2.Current)))
						return false;
				}

				if (await e2.MoveNextAsync(token).ConfigureAwait(false))
					return false;
			}

			return true;
		}

		public static IAsyncEnumerable<T> SkipAsync<T>(this IAsyncEnumerable<T> source, int count)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));

			return new SkipAsyncEnumerable<T>(source, count);
		}

		public static IAsyncEnumerable<T> TakeAsync<T>(this IAsyncEnumerable<T> source, int count)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));

			return new TakeAsyncEnumerable<T>(source, count);
		}

		public static IAsyncEnumerable<T> WhereAsync<T>(this IAsyncEnumerable<T> source, Func<T, bool> predicate)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));

			return new WhereAsyncEnumerable<T>(source, predicate);
		}

		private sealed class SkipAsyncEnumerable<T> : AsyncEnumerableBase<T>
		{
			private readonly int _count;

			public SkipAsyncEnumerable(IAsyncEnumerable<T> source, int count) : base(source)
			{
				_count = count;
			}

			public override IAsyncEnumerator<T> GetAsyncEnumerator()
			{
				return new SkipAsyncEnumerator<T>(_source.GetAsyncEnumerator(), _count);
			}
		}

		private sealed class SkipAsyncEnumerator<T> : AsyncEnumeratorBase<T>
		{
			private readonly int _count;
			private int _index = 0;

			public SkipAsyncEnumerator(IAsyncEnumerator<T> source, int count) : base(source)
			{
				_count = count;
			}

			public override async Task<bool> MoveNextAsync(CancellationToken token)
			{
				while (_index < _count && await _source.MoveNextAsync(token).ConfigureAwait(false))
					_index++;

				return await _source.MoveNextAsync(token).ConfigureAwait(false);
			}

			public override void Reset()
			{
				base.Reset();
				_index = 0;
			}
		}

		private sealed class TakeAsyncEnumerable<T> : AsyncEnumerableBase<T>
		{
			private readonly int _count;

			public TakeAsyncEnumerable(IAsyncEnumerable<T> source, int count) : base(source)
			{
				_count = count;
			}

			public override IAsyncEnumerator<T> GetAsyncEnumerator()
			{
				return new TakeAsyncEnumerator<T>(_source.GetAsyncEnumerator(), _count);
			}
		}

		private sealed class TakeAsyncEnumerator<T> : AsyncEnumeratorBase<T>
		{
			private readonly int _count;
			private int _index = 0;

			public TakeAsyncEnumerator(IAsyncEnumerator<T> source, int count) : base(source)
			{
				_count = count;
			}

			public override Task<bool> MoveNextAsync(CancellationToken token)
			{
				if (_index < _count)
				{
					_index++;
					return _source.MoveNextAsync(token);
				}

				return Task.FromResult(false);
			}

			public override void Reset()
			{
				base.Reset();
				_index = 0;
			}
		}

		private sealed class WhereAsyncEnumerable<T> : AsyncEnumerableBase<T>
		{
			private readonly Func<T, bool> _predicate;

			public WhereAsyncEnumerable(IAsyncEnumerable<T> source, Func<T, bool> predicate) : base(source)
			{
				_predicate = predicate;
			}

			public override IAsyncEnumerator<T> GetAsyncEnumerator()
			{
				return new WhereAsyncEnumerator<T>(_source.GetAsyncEnumerator(), _predicate);
			}
		}

		private sealed class WhereAsyncEnumerator<T> : AsyncEnumeratorBase<T>
		{
			private readonly Func<T, bool> _predicate;

			public WhereAsyncEnumerator(IAsyncEnumerator<T> source, Func<T, bool> predicate) : base(source)
			{
				_predicate = predicate;
			}
			
			public override async Task<bool> MoveNextAsync(CancellationToken token)
			{
				while (await _source.MoveNextAsync(token).ConfigureAwait(false))
				{
					if (_predicate.Invoke(Current))
						return true;
				}

				return false;
			}
		}

		private abstract class AsyncEnumerableBase<T> : IAsyncEnumerable<T>
		{
			protected readonly IAsyncEnumerable<T> _source;

			public AsyncEnumerableBase(IAsyncEnumerable<T> source)
			{
				_source = source;
			}

			public abstract IAsyncEnumerator<T> GetAsyncEnumerator();
		}

		private abstract class AsyncEnumeratorBase<T> : IAsyncEnumerator<T>
		{
			protected readonly IAsyncEnumerator<T> _source;

			public AsyncEnumeratorBase(IAsyncEnumerator<T> source)
			{
				_source = source;
			}

			public T Current => _source.Current;

			public void Dispose()
			{
				_source.Dispose();
			}

			public abstract Task<bool> MoveNextAsync(CancellationToken token);

			public virtual void Reset()
			{
				_source.Reset();
			}
		}
	}
}
