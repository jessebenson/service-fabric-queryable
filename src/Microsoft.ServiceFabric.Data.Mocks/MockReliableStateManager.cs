using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;

namespace Microsoft.ServiceFabric.Data.Mocks
{
	public class MockReliableStateManager : IReliableStateManager
	{
		private readonly ConcurrentDictionary<Uri, IReliableState> _state = new ConcurrentDictionary<Uri, IReliableState>();

		private IDictionary<Type, Type> _stateTypeMap = new Dictionary<Type, Type>
		{
			{ typeof(IReliableDictionary<,>), typeof(MockReliableDictionary<,>) },
			{ typeof(IReliableDictionary2<,>), typeof(MockReliableDictionary<,>) },
		};

		public event EventHandler<NotifyTransactionChangedEventArgs> TransactionChanged;
		public event EventHandler<NotifyStateManagerChangedEventArgs> StateManagerChanged;

		public ITransaction CreateTransaction()
		{
			return new MockTransaction();
		}

		public IAsyncEnumerator<IReliableState> GetAsyncEnumerator()
		{
			return new MockAsyncEnumerator<IReliableState>(_state.Values.GetEnumerator());
		}

		public Task<T> GetOrAddAsync<T>(string name) where T : IReliableState
		{
			return GetOrAddAsync<T>(GetUri(name));
		}

		public Task<T> GetOrAddAsync<T>(string name, TimeSpan timeout) where T : IReliableState
		{
			return GetOrAddAsync<T>(GetUri(name), timeout);
		}

		public Task<T> GetOrAddAsync<T>(ITransaction tx, string name) where T : IReliableState
		{
			return GetOrAddAsync<T>(tx, GetUri(name));
		}

		public Task<T> GetOrAddAsync<T>(ITransaction tx, string name, TimeSpan timeout) where T : IReliableState
		{
			return GetOrAddAsync<T>(tx, GetUri(name), timeout);
		}

		public Task<T> GetOrAddAsync<T>(Uri name) where T : IReliableState
		{
			return GetOrAddAsync<T>(name, TimeSpan.Zero);
		}

		public async Task<T> GetOrAddAsync<T>(Uri name, TimeSpan timeout) where T : IReliableState
		{
			using (var tx = CreateTransaction())
			{
				var result = await GetOrAddAsync<T>(tx, name, timeout).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
				return result;
			}
		}

		public Task<T> GetOrAddAsync<T>(ITransaction tx, Uri name) where T : IReliableState
		{
			return GetOrAddAsync<T>(tx, name, TimeSpan.Zero);
		}

		public Task<T> GetOrAddAsync<T>(ITransaction tx, Uri name, TimeSpan timeout) where T : IReliableState
		{
			return Task.FromResult((T)_state.GetOrAdd(name, n => CreateReliableState<T>(n)));
		}

		public Task RemoveAsync(string name)
		{
			return RemoveAsync(GetUri(name));
		}

		public Task RemoveAsync(string name, TimeSpan timeout)
		{
			return RemoveAsync(GetUri(name), timeout);
		}

		public Task RemoveAsync(ITransaction tx, string name)
		{
			return RemoveAsync(tx, GetUri(name));
		}

		public Task RemoveAsync(ITransaction tx, string name, TimeSpan timeout)
		{
			return RemoveAsync(tx, GetUri(name), timeout);
		}

		public Task RemoveAsync(Uri name)
		{
			return RemoveAsync(name, TimeSpan.Zero);
		}

		public async Task RemoveAsync(Uri name, TimeSpan timeout)
		{
			using (var tx = CreateTransaction())
			{
				await RemoveAsync(tx, name, timeout).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
			}
		}

		public Task RemoveAsync(ITransaction tx, Uri name)
		{
			return RemoveAsync(tx, name, TimeSpan.Zero);
		}

		public Task RemoveAsync(ITransaction tx, Uri name, TimeSpan timeout)
		{
			if (!_state.TryRemove(name, out IReliableState value))
				throw new KeyNotFoundException();

			return Task.CompletedTask;
		}

		public bool TryAddStateSerializer<T>(IStateSerializer<T> stateSerializer)
		{
			throw new NotImplementedException();
		}

		public Task<ConditionalValue<T>> TryGetAsync<T>(string name) where T : IReliableState
		{
			return TryGetAsync<T>(GetUri(name));
		}

		public Task<ConditionalValue<T>> TryGetAsync<T>(Uri name) where T : IReliableState
		{
			if (_state.TryGetValue(name, out IReliableState value))
				return Task.FromResult(new ConditionalValue<T>(true, (T)value));

			return Task.FromResult(new ConditionalValue<T>());
		}

		private T CreateReliableState<T>(Uri name) where T : IReliableState
		{
			var t = typeof(T);
			if (!_stateTypeMap.TryGetValue(t.GetGenericTypeDefinition(), out Type stateType))
				throw new ArgumentException();

			return (T)Activator.CreateInstance(stateType.MakeGenericType(t.GetGenericArguments()), name);
		}

		private static Uri GetUri(string name)
		{
			return new Uri($"urn://{name}");
		}
	}
}
