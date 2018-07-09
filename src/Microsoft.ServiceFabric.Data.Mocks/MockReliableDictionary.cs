using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;

namespace Microsoft.ServiceFabric.Data.Mocks
{
	public sealed class MockReliableDictionary<TKey, TValue> : IReliableDictionary2<TKey, TValue>
		where TKey : IComparable<TKey>, IEquatable<TKey>
	{
		private ConcurrentDictionary<TKey, TValue> _state = new ConcurrentDictionary<TKey, TValue>();

		public Func<IReliableDictionary<TKey, TValue>, NotifyDictionaryRebuildEventArgs<TKey, TValue>, Task> RebuildNotificationAsyncCallback { set => throw new NotImplementedException(); }

		public Uri Name { get; }

		public long Count => _state.Count;

		public event EventHandler<NotifyDictionaryChangedEventArgs<TKey, TValue>> DictionaryChanged;

		public MockReliableDictionary(Uri name)
		{
			Name = name;
		}

		public Task AddAsync(ITransaction tx, TKey key, TValue value)
		{
			return AddAsync(tx, key, value, TimeSpan.Zero, CancellationToken.None);
		}

		public Task AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			if (!_state.TryAdd(key, value))
				throw new ArgumentException("Key already exists.", nameof(key));

			return Task.CompletedTask;
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
		{
			return AddOrUpdateAsync(tx, key, addValue, updateValueFactory, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return AddOrUpdateAsync(tx, key, k => addValue, updateValueFactory, timeout, cancellationToken);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
		{
			return AddOrUpdateAsync(tx, key, addValueFactory, updateValueFactory, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult(_state.AddOrUpdate(key, addValueFactory, updateValueFactory));
		}

		public Task ClearAsync()
		{
			return ClearAsync(TimeSpan.Zero, CancellationToken.None);
		}

		public Task ClearAsync(TimeSpan timeout, CancellationToken cancellationToken)
		{
			_state.Clear();
			return Task.CompletedTask;
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key)
		{
			return ContainsKeyAsync(tx, key, LockMode.Default);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode)
		{
			return ContainsKeyAsync(tx, key, LockMode.Default, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return ContainsKeyAsync(tx, key, LockMode.Default, timeout, cancellationToken);
		}

		public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult(_state.ContainsKey(key));
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx)
		{
			return CreateEnumerableAsync(tx, EnumerationMode.Unordered);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
		{
			return CreateEnumerableAsync(tx, k => true, enumerationMode);
		}

		public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction tx, Func<TKey, bool> filter, EnumerationMode enumerationMode)
		{
			return Task.FromResult<IAsyncEnumerable<KeyValuePair<TKey, TValue>>>(
				new MockAsyncEnumerable<KeyValuePair<TKey, TValue>>(
					enumerationMode == EnumerationMode.Ordered
						? _state.Where(x => filter(x.Key)).OrderBy(x => x.Key)
						: _state.Where(x => filter(x.Key))));
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx)
		{
			throw new NotImplementedException();
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode)
		{
			throw new NotImplementedException();
		}

		public Task<IAsyncEnumerable<TKey>> CreateKeyEnumerableAsync(ITransaction tx, EnumerationMode enumerationMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult<IAsyncEnumerable<TKey>>(
				new MockAsyncEnumerable<TKey>(
					enumerationMode == EnumerationMode.Ordered
					? _state.Select(x => x.Key).OrderBy(k => k)
					: _state.Select(x => x.Key)));
		}

		public Task<long> GetCountAsync(ITransaction tx)
		{
			return Task.FromResult((long) _state.Count);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value)
		{
			return GetOrAddAsync(tx, key, value, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return GetOrAddAsync(tx, key, k => value, timeout, cancellationToken);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory)
		{
			return GetOrAddAsync(tx, key, valueFactory, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult(_state.GetOrAdd(key, valueFactory));
		}

		public Task SetAsync(ITransaction tx, TKey key, TValue value)
		{
			return SetAsync(tx, key, value, TimeSpan.Zero, CancellationToken.None);
		}

		public Task SetAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			_state[key] = value;
			return Task.CompletedTask;
		}

		public Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value)
		{
			return TryAddAsync(tx, key, value, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult(_state.TryAdd(key, value));
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key)
		{
			return TryGetValueAsync(tx, key, LockMode.Default);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode)
		{
			return TryGetValueAsync(tx, key, lockMode, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return TryGetValueAsync(tx, key, LockMode.Default, timeout, cancellationToken);
		}

		public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
		{
			if (_state.TryGetValue(key, out TValue value))
				return Task.FromResult(new ConditionalValue<TValue>(true, value));

			return Task.FromResult(new ConditionalValue<TValue>());
		}

		public Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key)
		{
			return TryRemoveAsync(tx, key, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
		{
			if (_state.TryRemove(key, out TValue value))
				return Task.FromResult(new ConditionalValue<TValue>(true, value));

			return Task.FromResult(new ConditionalValue<TValue>());
		}

		public Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue)
		{
			return TryUpdateAsync(tx, key, newValue, comparisonValue, TimeSpan.Zero, CancellationToken.None);
		}

		public Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue, TimeSpan timeout, CancellationToken cancellationToken)
		{
			return Task.FromResult(_state.TryUpdate(key, newValue, comparisonValue));
		}
	}
}
