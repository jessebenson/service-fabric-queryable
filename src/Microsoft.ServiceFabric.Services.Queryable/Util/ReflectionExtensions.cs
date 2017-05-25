using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ReflectionExtensions
	{
		/// <summary>
		/// Determines if the object instance implements the generic interface type.
		/// </summary>
		/// <param name="instance">Object instance to verify.</param>
		/// <param name="genericType">Generic interface type to check for.</param>
		/// <returns>True if the type of instance implements the given generic interface type.</returns>
		public static bool ImplementsGenericType(this object instance, Type genericType)
		{
			if (!genericType.IsGenericType || !genericType.IsInterface)
				throw new ArgumentException(nameof(genericType));

			return instance.GetType().GetInterfaces().Where(i => i.IsGenericType).Any(i => i.GetGenericTypeDefinition() == genericType);
		}

		/// <summary>
		/// Reads all values from the reliable state into an in-memory collection.
		/// </summary>
		/// <param name="state">Reliable state (must implement <see cref="IReliableDictionary{TKey, TValue}"/>).</param>
		/// <param name="stateManager">Reliable state manager, to create a transaction.</param>
		/// <returns>All values from the reliable state in an in-memory collection.</returns>
		public static async Task<IEnumerable<object>> ToEnumerable(this IReliableState state, IReliableStateManager stateManager)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			var results = new List<object>();
			using (var tx = stateManager.CreateTransaction())
			{
				// Create the async enumerable.
				var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(state.GetType().GetGenericArguments());
				var createEnumerableAsyncTask = state.CallMethod<Task>("CreateEnumerableAsync", new[] { typeof(ITransaction) }, tx);
				await createEnumerableAsyncTask.ConfigureAwait(false);

				var asyncEnumerable = createEnumerableAsyncTask.GetPropertyValue<object>("Result");
				var asyncEnumerator = asyncEnumerable.CallMethod<object>("GetAsyncEnumerator");

				// Copy all items from the reliable dictionary into memory.
				while (await asyncEnumerator.CallMethod<Task<bool>>("MoveNextAsync", CancellationToken.None).ConfigureAwait(false))
				{
					var current = asyncEnumerator.GetPropertyValue<object>("Current");
					var value = current.GetPropertyValue<object>("Value");
					results.Add(value);
				}
			}

			return results;
		}

		/// <summary>
		/// Casts the given object enumerable to an enumerable of the given type.
		/// </summary>
		/// <param name="enumerable">Object enumerable to cast.</param>
		/// <param name="type"><paramref name="enumerable"/> will be cast to an enumerable of this type.</param>
		/// <returns>Typed enumerable.</returns>
		public static IEnumerable CastEnumerable(this IEnumerable<object> enumerable, Type type)
		{
			var castMethod = typeof(Enumerable).GetMethod("Cast", BindingFlags.Static | BindingFlags.Public);
			var castGenericMethod = castMethod.MakeGenericMethod(type);
			return (IEnumerable)castGenericMethod.Invoke(null, new object[] { enumerable });
		}

		/// <summary>
		/// Get the key type from the reliable dictionary.
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The type of the dictionary's keys.</returns>
		public static Type GetKeyType(this IReliableState state)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			return state.GetType().GetGenericArguments()[0];
		}

		/// <summary>
		/// Get the value type from the reliable dictionary.
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The type of the dictionary's values.</returns>
		public static Type GetValueType(this IReliableState state)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			return state.GetType().GetGenericArguments()[1];
		}

		public static IEnumerable<object> ToEnumerable(this IQueryable queryable)
		{
			var enumerator = queryable.GetEnumerator();
			while (enumerator.MoveNext())
			{
				yield return enumerator.Current;
			}
		}

		public static Type GetTypeByName(string className)
		{
			foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
			{
				var assemblyTypes = a.GetTypes();
				foreach (Type t in assemblyTypes)
				{
					if (t.Name == className)
					{
						return t;
					}
				}
			}

			return null;
		}

		private static TReturn CallMethod<TReturn>(this object instance, string methodName, params object[] parameters)
		{
			return (TReturn)instance.GetType().GetMethod(methodName).Invoke(instance, parameters);
		}

		private static TReturn CallMethod<TReturn>(this object instance, string methodName, Type[] parameterTypes, params object[] parameters)
		{
			return (TReturn)instance.GetType().GetMethod(methodName, parameterTypes).Invoke(instance, parameters);
		}

		private static TReturn GetPropertyValue<TReturn>(this object instance, string propertyName)
		{
			var property = instance.GetType().GetProperty(propertyName);
			return (TReturn)property?.GetValue(instance);
		}
	}
}
