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
		public static async Task<IEnumerable<TSource>> AsEnumerable<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken token = default(CancellationToken))
		{
			var results = new List<TSource>();
			using (var enumerator = source.GetAsyncEnumerator())
			{
				while (await enumerator.MoveNextAsync(token).ConfigureAwait(false))
					results.Add(enumerator.Current);
			}

			return results;
		}

		public static IAsyncEnumerable<TSource> AsAsyncEnumerable<TSource>(this IEnumerable<TSource> source)
		{
			return new DefaultAsyncEnumerable<TSource>(source);
		}

		public static IAsyncEnumerable<TResult> CastAsync<TSource, TResult>(this IAsyncEnumerable<TSource> source)
		{
			IAsyncEnumerable<TResult> typedSource = source as IAsyncEnumerable<TResult>;
			if (typedSource != null) return typedSource;
			if (source == null) throw new ArgumentNullException(nameof(source));

			return new SelectAsyncEnumerable<TSource, TResult>(source, obj => (TResult)Convert.ChangeType(obj, typeof(TResult)));
		}

		public static IAsyncEnumerable<object> CastAsync<TSource>(this IAsyncEnumerable<TSource> source, Type resultType)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (resultType == null) throw new ArgumentNullException(nameof(source));

			return new SelectAsyncEnumerable<TSource, object>(source, obj => Convert.ChangeType(obj, resultType));
		}

		public static Task<bool> ContainsAsync<TSource>(this IAsyncEnumerable<TSource> source, TSource value, CancellationToken token = default(CancellationToken))
		{
			return ContainsAsync(source, value, null, token);
		}

		public static async Task<bool> ContainsAsync<TSource>(this IAsyncEnumerable<TSource> source, TSource value, IEqualityComparer<TSource> comparer, CancellationToken token = default(CancellationToken))
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (comparer == null) comparer = EqualityComparer<TSource>.Default;

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

		public static Task<int> CountAsync<TSource>(this IAsyncEnumerable<TSource> source, CancellationToken token = default(CancellationToken))
		{
			return CountAsync(source, _ => true, token);
		}

		public static async Task<int> CountAsync<TSource>(this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, CancellationToken token = default(CancellationToken))
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

		public static IAsyncEnumerable<TSource> EmptyAsync<TSource>()
		{
			return Enumerable.Empty<TSource>().AsAsyncEnumerable();
		}

		public static IAsyncEnumerable<TResult> SelectAsync<TSource, TResult>(this IAsyncEnumerable<TSource> source, Func<TSource, TResult> selector)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (selector == null) throw new ArgumentNullException(nameof(selector));

			return new SelectAsyncEnumerable<TSource, TResult>(source, selector);
		}

		public static Task<bool> SequenceEqualAsync<TSource>(this IAsyncEnumerable<TSource> first, IAsyncEnumerable<TSource> second, CancellationToken token = default(CancellationToken))
		{
			return SequenceEqualAsync(first, second, null, token);
		}

		public static async Task<bool> SequenceEqualAsync<TSource>(this IAsyncEnumerable<TSource> first, IAsyncEnumerable<TSource> second, IEqualityComparer<TSource> comparer, CancellationToken token = default(CancellationToken))
		{
			if (first == null) throw new ArgumentNullException(nameof(first));
			if (second == null) throw new ArgumentNullException(nameof(second));
			if (comparer == null) comparer = EqualityComparer<TSource>.Default;

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

		public static IAsyncEnumerable<TSource> SkipAsync<TSource>(this IAsyncEnumerable<TSource> source, int count)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));

			return new SkipAsyncEnumerable<TSource>(source, count);
		}

		public static IAsyncEnumerable<TSource> TakeAsync<TSource>(this IAsyncEnumerable<TSource> source, int count)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));

			return new TakeAsyncEnumerable<TSource>(source, count);
		}

		public static IAsyncEnumerable<TSource> WhereAsync<TSource>(this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
		{
			if (source == null) throw new ArgumentNullException(nameof(source));
			if (predicate == null) throw new ArgumentNullException(nameof(predicate));

			return new WhereAsyncEnumerable<TSource>(source, predicate);
		}

		private sealed class SelectAsyncEnumerable<TSource, TResult> : IAsyncEnumerable<TResult>
		{
			private readonly IAsyncEnumerable<TSource> _source;
			private readonly Func<TSource, TResult> _selector;

			public SelectAsyncEnumerable(IAsyncEnumerable<TSource> source, Func<TSource, TResult> selector)
			{
				_source = source;
				_selector = selector;
			}

			public IAsyncEnumerator<TResult> GetAsyncEnumerator()
			{
				return new SelectAsyncEnumerator<TSource, TResult>(_source.GetAsyncEnumerator(), _selector);
			}
		}

		private sealed class SelectAsyncEnumerator<TSource, TResult> : IAsyncEnumerator<TResult>
		{
			private readonly IAsyncEnumerator<TSource> _source;
			private readonly Func<TSource, TResult> _selector;

			public SelectAsyncEnumerator(IAsyncEnumerator<TSource> source, Func<TSource, TResult> selector)
			{
				_source = source;
				_selector = selector;
			}

			public TResult Current => _selector(_source.Current);

			public void Dispose()
			{
				_source.Dispose();
			}

			public Task<bool> MoveNextAsync(CancellationToken token)
			{
				return _source.MoveNextAsync(token);
			}

			public void Reset()
			{
				_source.Reset();
			}
		}

		private sealed class SkipAsyncEnumerable<TSource> : IAsyncEnumerable<TSource>
		{
			private readonly IAsyncEnumerable<TSource> _source;
			private readonly int _count;

			public SkipAsyncEnumerable(IAsyncEnumerable<TSource> source, int count)
			{
				_source = source;
				_count = count;
			}

			public IAsyncEnumerator<TSource> GetAsyncEnumerator()
			{
				return new SkipAsyncEnumerator<TSource>(_source.GetAsyncEnumerator(), _count);
			}
		}

		private sealed class SkipAsyncEnumerator<TSource> : AsyncEnumeratorBase<TSource>
		{
			private readonly int _count;
			private int _index = 0;

			public SkipAsyncEnumerator(IAsyncEnumerator<TSource> source, int count) : base(source)
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

		private sealed class TakeAsyncEnumerable<TSource> : IAsyncEnumerable<TSource>
		{
			private readonly IAsyncEnumerable<TSource> _source;
			private readonly int _count;

			public TakeAsyncEnumerable(IAsyncEnumerable<TSource> source, int count)
			{
				_source = source;
				_count = count;
			}

			public IAsyncEnumerator<TSource> GetAsyncEnumerator()
			{
				return new TakeAsyncEnumerator<TSource>(_source.GetAsyncEnumerator(), _count);
			}
		}

		private sealed class TakeAsyncEnumerator<TSource> : AsyncEnumeratorBase<TSource>
		{
			private readonly int _count;
			private int _index = 0;

			public TakeAsyncEnumerator(IAsyncEnumerator<TSource> source, int count) : base(source)
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

		private sealed class WhereAsyncEnumerable<TSource> : IAsyncEnumerable<TSource>
		{
			private readonly IAsyncEnumerable<TSource> _source;
			private readonly Func<TSource, bool> _predicate;

			public WhereAsyncEnumerable(IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate)
			{
				_source = source;
				_predicate = predicate;
			}

			public IAsyncEnumerator<TSource> GetAsyncEnumerator()
			{
				return new WhereAsyncEnumerator<TSource>(_source.GetAsyncEnumerator(), _predicate);
			}
		}

		private sealed class WhereAsyncEnumerator<TSource> : AsyncEnumeratorBase<TSource>
		{
			private readonly Func<TSource, bool> _predicate;

			public WhereAsyncEnumerator(IAsyncEnumerator<TSource> source, Func<TSource, bool> predicate) : base(source)
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

		private abstract class AsyncEnumeratorBase<TSource> : IAsyncEnumerator<TSource>
		{
			protected readonly IAsyncEnumerator<TSource> _source;

			public AsyncEnumeratorBase(IAsyncEnumerator<TSource> source)
			{
				_source = source;
			}

			public TSource Current => _source.Current;

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
