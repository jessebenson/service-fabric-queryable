using Microsoft.Data.Edm;
using Microsoft.Data.OData;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData.Query;


namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ReliableStateExtensions
	{
		private static readonly QueryModelCache QueryCache = new QueryModelCache();

		/// <summary>
		/// Get the OData metadata about the reliable collections from the reliable state manager using reflection.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <returns>The OData metadata for this state manager.</returns>
		public static async Task<string> GetMetadataAsync(this IReliableStateManager stateManager)
		{
			// Build the OData model from the queryable types in the reliable state manager.
			var builder = new ODataConventionModelBuilder();
			foreach (var queryable in await stateManager.GetQueryableTypes().ConfigureAwait(false))
			{
				var entity = builder.AddEntity(queryable.Value);
				builder.AddEntitySet(queryable.Key, entity);
			}

			var model = builder.GetEdmModel();

			// Write the OData metadata document.
			using (var stream = new MemoryStream())
			using (var message = new InMemoryMessage {Stream = stream})
			{
				var settings = new ODataMessageWriterSettings();
				var writer = new ODataMessageWriter((IODataResponseMessage) message, settings, model);
				writer.WriteMetadataDocument();
				return Encoding.UTF8.GetString(stream.ToArray());
			}
		}

		/// <summary>
		/// Query the reliable collection with the given name from the reliable state manager using the given query parameters.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="context">Stateful Service Context.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The json serialized results of the query.</returns>
		public static async Task<IEnumerable<string>> QueryAsync(this IReliableStateManager stateManager,
			StatefulServiceContext context, string collection, IEnumerable<KeyValuePair<string, string>> query,
			CancellationToken cancellationToken)
		{
			// Query all service partitions concurrently.
			var proxies = await GetServiceProxiesAsync<IQueryableService>(context).ConfigureAwait(false);
			var queries = proxies.Select(p => p.QueryPartitionAsync(collection, query)).Concat(new[]
				{stateManager.QueryPartitionAsync(collection, query, context.PartitionId, cancellationToken)});
			var queryResults = await Task.WhenAll(queries).ConfigureAwait(false);
			var results = queryResults.SelectMany(r => r);


			// Run the aggregation query to get the final results (e.g. for top, orderby, project).
			if (query.Any())
			{
				var reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);
				var entityType = reliableState.GetEntityType();
				// var temp= results.Select(x=>JsonConvert.DeserializeObject(x,))


				//var output = results.Select(r => new Tuple<Guid, object>(partitionId, r));

				// JsonConvert.DeserializeObject<Guid, object>(results.Select(r => r));

				var objects = results.Select(r => JsonConvert.DeserializeObject(r, entityType));
				var queryResult = ApplyQuery(objects, entityType, query, aggregate: true);
				results = queryResult.Select(JsonConvert.SerializeObject);
			}

			// Return the filtered data as json.
			return results;
		}

		/// <summary>
		/// Query the reliable collection with the given name from the reliable state manager using the given query parameters.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The json serialized results of the query.</returns>
		public static async Task<IEnumerable<string>> QueryPartitionAsync(this IReliableStateManager stateManager,
			string collection, IEnumerable<KeyValuePair<string, string>> query, Guid partitionId,
			CancellationToken cancellationToken)
		{
			// Find the reliable state.
			var reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);

			// Get the data from the reliable state.
			var results = await reliableState.GetEnumerable(stateManager, partitionId, cancellationToken).ConfigureAwait(false);

			// Filter the data.
			if (query.Any())
			{
				var entityType = reliableState.GetEntityType();
				results = ApplyQuery(results, entityType, query, aggregate: false);
			}

			// Return the filtered data as json.
			return results.Select(JsonConvert.SerializeObject);
		}

		/// <summary>
		/// Delete from the reliable collection with the given key from the reliable state manager using the given parameters.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="keyJson">Entity Key.</param>
		/// <returns>A boolean value based on the success of the operation.</returns>
		public static async Task<bool> DeleteAsync(this IReliableStateManager stateManager, string collection,
			string keyJson)
		{

			var dictionary = await stateManager.GetQueryableState(collection).ConfigureAwait(false);
			try
			{
				using (ITransaction tx = stateManager.CreateTransaction())
				{
					var keyType = dictionary.GetKeyType();
					var valueType = dictionary.GetValueType();
					var key = JsonConvert.DeserializeObject(keyJson, keyType);
					var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);
					await (Task) dictionaryType.GetMethod("TryRemoveAsync", new[] {typeof(ITransaction), keyType})
						.Invoke(dictionary, new[] {tx, key});

					await tx.CommitAsync();

				}
			}
			catch (ArgumentException)
			{
				throw new HttpException((int)HttpStatusCode.BadRequest, $"A value with given key:{keyJson} does not exist.");
			}

			return true;

		}

		/// <summary>
		/// Add to the reliable collection the given key & value using the reliable state managers.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="keyJson">Entity Key.</param>
		/// <param name="valJson">Value.</param>
		/// <returns>A boolean value based on the success of the operation.</returns>
		public static async Task<bool> AddAsync(this IReliableStateManager stateManager, string collection, string keyJson,
			string valJson)
		{

			var dictionary = await stateManager.GetQueryableState(collection).ConfigureAwait(false);

			try
			{
				using (ITransaction tx = stateManager.CreateTransaction())
				{
					var keyType = dictionary.GetKeyType();
					var valueType = dictionary.GetValueType();
					var key = JsonConvert.DeserializeObject(keyJson, keyType);
					var val = JsonConvert.DeserializeObject(valJson, valueType);

					var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);
					await (Task) dictionaryType.GetMethod("AddAsync", new[] {typeof(ITransaction), keyType, valueType})
						.Invoke(dictionary, new[] {tx, key, val});

					await tx.CommitAsync();

				}
			}
			catch (ArgumentException)
			{
				throw new HttpException((int) HttpStatusCode.BadRequest, "A value with same key already exists.");
			}

			return true;
		}

		public static async Task<bool> UpdateAsync(this IReliableStateManager stateManager, string collection,
			string keyJson, string valJson)
		{

			var dictionary = await stateManager.GetQueryableState(collection).ConfigureAwait(false);

			try
			{
				using (ITransaction tx = stateManager.CreateTransaction())
				{
					var keyType = dictionary.GetKeyType();
					var valueType = dictionary.GetValueType();

					var key = JsonConvert.DeserializeObject(keyJson, keyType);
					var val = JsonConvert.DeserializeObject(valJson, valueType);

					var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

					await (Task) dictionaryType.GetMethod("SetAsync", new[] {typeof(ITransaction), keyType, valueType})
						.Invoke(dictionary, new[] {tx, key, val});

					await tx.CommitAsync();

				}
			}
			catch (ArgumentException)
			{

				throw new HttpException((int)HttpStatusCode.BadRequest, "Updating to same value again.");

			}

			return true;
		}

		/// <summary>
		/// Get the queryable reliable collection by name.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <returns>The reliable collection that supports querying.</returns>
		private static async Task<IReliableState> GetQueryableState(this IReliableStateManager stateManager,
			string collection)
		{
			// Find the reliable state.
			var reliableStateResult = await stateManager.TryGetAsync<IReliableState>(collection).ConfigureAwait(false);
			if (!reliableStateResult.HasValue)
				throw new ArgumentException($"IReliableState '{collection}' not found.");

			// Verify the state is a reliable dictionary.
			var reliableState = reliableStateResult.Value;
			if (!reliableState.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException($"IReliableState '{collection}' must be an IReliableDictionary.");

			return reliableState;
		}

		/// <summary>
		/// Get the names and types of the reliable collections that are queryable from the reliable state manager.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The names and value types of the reliable collections that are queryable.</returns>
		private static async Task<IEnumerable<KeyValuePair<string, Type>>> GetQueryableTypes(
			this IReliableStateManager stateManager, CancellationToken cancellationToken = default(CancellationToken))
		{
			var types = new Dictionary<string, Type>();
			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
			{
				var state = enumerator.Current;
				if (state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				{
					var entityType = state.GetEntityType();
					types.Add(state.Name.AbsolutePath, entityType);
				}
			}

			return types;
		}

		private static Type GetEntityType(this IReliableState state)
		{
			var keyType = state.GetKeyType();
			var valueType = state.GetValueType();
			return typeof(Entity<,>).MakeGenericType(keyType, valueType);
		}

		/// <summary>
		/// Get the key type from the reliable dictionary.
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The type of the dictionary's keys.</returns>
		private static Type GetKeyType(this IReliableState state)
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
		private static Type GetValueType(this IReliableState state)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			return state.GetType().GetGenericArguments()[1];
		}

		/// <summary>
		/// Reads all values from the reliable state into an in-memory collection.
		/// </summary>
		/// <param name="state">Reliable state (must implement <see cref="IReliableDictionary{TKey, TValue}"/>).</param>
		/// <param name="stateManager">Reliable state manager, to create a transaction.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>All values from the reliable state in an in-memory collection.</returns>
		private static async Task<IEnumerable<object>> GetEnumerable(this IReliableState state,
			IReliableStateManager stateManager, Guid partitionId, CancellationToken cancellationToken)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			var entityType = state.GetEntityType();

			var results = new List<object>();
			using (var tx = stateManager.CreateTransaction())
			{
				// Create the async enumerable.
				var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(state.GetType().GetGenericArguments());
				var createEnumerableAsyncTask = state.CallMethod<Task>("CreateEnumerableAsync", new[] {typeof(ITransaction)}, tx);
				await createEnumerableAsyncTask.ConfigureAwait(false);

				var asyncEnumerable = createEnumerableAsyncTask.GetPropertyValue<object>("Result");
				var asyncEnumerator = asyncEnumerable.CallMethod<object>("GetAsyncEnumerator");

				// Copy all items from the reliable dictionary into memory.
				// TODO: cache the method/property objects and invoke with new parameters
				while (await asyncEnumerator.CallMethod<Task<bool>>("MoveNextAsync", cancellationToken).ConfigureAwait(false))
				{
					var current = asyncEnumerator.GetPropertyValue<object>("Current");
					var key = current.GetPropertyValue<object>("Key");
					var value = current.GetPropertyValue<object>("Value");

					var entity = Activator.CreateInstance(entityType);
					entity.SetPropertyValue("PartitionId", partitionId);
					entity.SetPropertyValue("Key", key);
					entity.SetPropertyValue("Value", value);

					results.Add(entity);
				}
			}

			return results;
		}

		private static async Task<IEnumerable<T>> GetServiceProxiesAsync<T>(StatefulServiceContext context) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(context.ServiceName).ConfigureAwait(false);
				return partitions.Where(p => p.PartitionInformation.Id != context.PartitionId)
					.Select(p => CreateServiceProxy<T>(context.ServiceName, p));
			}
		}

		private static T CreateServiceProxy<T>(Uri serviceUri, Partition partition) where T : IService
		{
			if (partition.PartitionInformation is Int64RangePartitionInformation)
				return ServiceProxy.Create<T>(serviceUri,
					new ServicePartitionKey(((Int64RangePartitionInformation) partition.PartitionInformation).LowKey));
			if (partition.PartitionInformation is NamedPartitionInformation)
				return ServiceProxy.Create<T>(serviceUri,
					new ServicePartitionKey(((NamedPartitionInformation) partition.PartitionInformation).Name));
			if (partition.PartitionInformation is SingletonPartitionInformation)
				return ServiceProxy.Create<T>(serviceUri);

			throw new ArgumentException(nameof(partition));
		}

		/// <summary>
		/// Apply the OData query specified by <paramref name="query"/> to the in-memory objects.
		/// </summary>
		/// <param name="data">The in-memory objects to query.</param>
		/// <param name="type">The Type of the objects in <paramref name="data"/>.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="aggregate">Indicates whether this is an aggregation or partial query.</param>
		/// <returns>The results of applying the query to the in-memory objects.</returns>
		private static IEnumerable<object> ApplyQuery(IEnumerable<object> data, Type type,
			IEnumerable<KeyValuePair<string, string>> query, bool aggregate)
		{
			// Get the OData query context for this type.
			var context = QueryCache.GetQueryContext(type);

			// Cast to correct IQueryable type.
			var casted = data.CastEnumerable(type);

			// Execute the query.
			var options = new ODataQueryOptions(query, context, aggregate);
			var settings = new ODataQuerySettings {HandleNullPropagation = HandleNullPropagationOption.True};
			var result = options.ApplyTo(casted.AsQueryable(), settings);

			// Get the query results.
			return result.Cast<object>();
		}
	}
}
