using Microsoft.Data.OData;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
			using (var message = new InMemoryMessage { Stream = stream })
			{
				var settings = new ODataMessageWriterSettings();
				var writer = new ODataMessageWriter((IODataResponseMessage)message, settings, model);
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
		public static async Task<IEnumerable<JToken>> QueryAsync(this IReliableStateManager stateManager,
			StatefulServiceContext context, string collection, IEnumerable<KeyValuePair<string, string>> query, CancellationToken cancellationToken)
		{
			// Get the list of partitions (excluding the executing partition).
			var partitions = await GetPartitionsAsync(context).ConfigureAwait(false);

			// Query all service partitions concurrently.
			var remoteQueries = partitions.Select(p => QueryPartitionAsync(p, context, collection, query, cancellationToken));
			var localQuery = stateManager.QueryPartitionAsync(collection, query, context.PartitionId, cancellationToken);
			var queries = remoteQueries.Concat(new[] { localQuery });

			// Aggregate all query results into a single list.
			var queryResults = await Task.WhenAll(queries).ConfigureAwait(false);
			var results = queryResults.SelectMany(r => r);

			// Run the aggregation query to get the final results (e.g. for top, orderby, project).
			if (queryResults.Length > 1)
			{
				var reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);
				var entityType = reliableState.GetEntityType();
				var objects = results.Select(r => r.ToObject(entityType));
				var queryResult = ApplyQuery(objects, entityType, query, aggregate: true);
				results = queryResult.Select(q => JObject.FromObject(q));
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
		public static async Task<IEnumerable<JToken>> QueryPartitionAsync(this IReliableStateManager stateManager,
			string collection, IEnumerable<KeyValuePair<string, string>> query, Guid partitionId, CancellationToken cancellationToken)
		{
			// Find the reliable state.
			var reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);

			using (var tx = stateManager.CreateTransaction())
			{
				// Get the data from the reliable state.
				var results = await reliableState.GetAsyncEnumerable(tx, stateManager, partitionId, cancellationToken).ConfigureAwait(false);

				// Filter the data.
				var entityType = reliableState.GetEntityType();
				results = ApplyQuery(results, entityType, query, aggregate: false);

				// Convert to json.
				var json = await results.SelectAsync(r => JObject.FromObject(r)).AsEnumerable().ConfigureAwait(false);

				await tx.CommitAsync().ConfigureAwait(false);

				// Return the filtered data as json.
				return json;
			}
		}

		/// <summary>
		/// This implementation should not leak into the core Queryable code.  It is dependent on the specific protocol
		/// used to communicate with the other partitions (HTTP over ReverseProxy), and should be hidden behind an interface.
		/// </summary>
		private static async Task<IEnumerable<JToken>> QueryPartitionAsync(Partition partition,
			StatefulServiceContext context, string collection, IEnumerable<KeyValuePair<string, string>> query, CancellationToken cancellationToken)
		{
			using (var client = new HttpClient { BaseAddress = new Uri("http://localhost:19081/") })
			{
				string requestUri = $"{context.ServiceName.AbsolutePath}/query/{partition.PartitionInformation.Id}/{collection}?{GetQueryParameters(query, partition)}";
				var response = await client.GetAsync(requestUri).ConfigureAwait(false);
				var content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

				var result = JsonConvert.DeserializeObject<ODataResult>(content);
				return result.Value;
			}
		}

		private static string GetQueryParameters(IEnumerable<KeyValuePair<string, string>> query, Partition partition)
		{
			var partitionParameters = GetPartitionQueryParameters(partition);
			var queryParameters = partitionParameters.Concat(query).Distinct();
			return string.Join("&", queryParameters.Select(p => $"{p.Key}={p.Value}"));
		}

		private static IEnumerable<KeyValuePair<string, string>> GetPartitionQueryParameters(Partition partition)
		{
			var info = partition.PartitionInformation;
			yield return new KeyValuePair<string, string>("PartitionKind", info.Kind.ToString());

			if (info.Kind == ServicePartitionKind.Int64Range)
				yield return new KeyValuePair<string, string>("PartitionKey", (info as Int64RangePartitionInformation).LowKey.ToString());
			else if (info.Kind == ServicePartitionKind.Named)
				yield return new KeyValuePair<string, string>("PartitionKey", (info as NamedPartitionInformation).Name);
		}

		/// <summary>
		/// Execute the operations given in <paramref name="operations"/> in a transaction.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="operations">Operations (add/update/delete) to perform against collections in the partition.</param>
		/// <returns>A list of status codes indicating success/failure of the operations.</returns>
		public static async Task<List<EntityOperationResult>> ExecuteAsync(this IReliableStateManager stateManager, EntityOperation<JToken, JToken>[] operations)
		{
			var results = new List<EntityOperationResult>();
			using (var tx = stateManager.CreateTransaction())
			{
				foreach (var operation in operations)
				{
					HttpStatusCode status = HttpStatusCode.BadRequest;

					try
					{
						// Get the reliable dictionary for this operation.
						var dictionary = await stateManager.GetQueryableState(operation.Collection).ConfigureAwait(false);

						// Execute operation.
						if (operation.Operation == Operation.Add)
						{
							status = await ExecuteAddAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
						else if (operation.Operation == Operation.Update)
						{
							status = await ExecuteUpdateAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
						else if (operation.Operation == Operation.Delete)
						{
							status = await ExecuteDeleteAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
					}
					catch (ArgumentException)
					{
						status = HttpStatusCode.BadRequest;
					}
					catch (Exception)
					{
						status = HttpStatusCode.InternalServerError;
					}

					// Add the operation result.
					results.Add(new EntityOperationResult
					{
						PartitionId = operation.PartitionId,
						Collection = operation.Collection,
						Key = operation.Key,
						Status = (int)status,
					});
				}

				await tx.CommitAsync().ConfigureAwait(false);
			}

			return results;
		}

		private static async Task<HttpStatusCode> ExecuteAddAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			Type keyType = dictionary.GetKeyType();
			Type valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			var value = operation.Value.ToObject(valueType);
			Type dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Add to reliable dictionary.
			MethodInfo tryAddMethod = dictionaryType.GetMethod("TryAddAsync", new[] { typeof(ITransaction), keyType, valueType });
			bool success = await ((Task<bool>)tryAddMethod.Invoke(dictionary, new[] { tx, key, value })).ConfigureAwait(false);

			return success ? HttpStatusCode.OK : HttpStatusCode.Conflict;
		}

		private static async Task<HttpStatusCode> ExecuteUpdateAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			Type keyType = dictionary.GetKeyType();
			Type valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			var value = operation.Value.ToObject(valueType);
			Type dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Read the existing value.
			MethodInfo tryGetMethod = dictionaryType.GetMethod("TryGetValueAsync", new[] { typeof(ITransaction), keyType, typeof(LockMode) });
			var tryGetTask = (Task)tryGetMethod.Invoke(dictionary, new[] { tx, key, LockMode.Update });
			await tryGetTask.ConfigureAwait(false);
			var tryGetResult = tryGetTask.GetPropertyValue<object>("Result");

			// Only update the value if it exists.
			if (!tryGetResult.GetPropertyValue<bool>("HasValue"))
				return HttpStatusCode.NotFound;

			// Validate the ETag.
			var currentValue = tryGetResult.GetPropertyValue<object>("Value");
			var currentEtag = CRC64.ToCRC64(JsonConvert.SerializeObject(currentValue)).ToString();
			if (currentEtag != operation.Etag)
				return HttpStatusCode.PreconditionFailed;

			// Update in reliable dictionary.
			MethodInfo setMethod = dictionaryType.GetMethod("SetAsync", new[] { typeof(ITransaction), keyType, valueType });
			await ((Task)setMethod.Invoke(dictionary, new[] { tx, key, value })).ConfigureAwait(false);

			return HttpStatusCode.OK;
		}

		private static async Task<HttpStatusCode> ExecuteDeleteAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			var keyType = dictionary.GetKeyType();
			var valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Read the existing value.
			MethodInfo tryGetMethod = dictionaryType.GetMethod("TryGetValueAsync", new[] { typeof(ITransaction), keyType, typeof(LockMode) });
			var tryGetTask = (Task)tryGetMethod.Invoke(dictionary, new[] { tx, key, LockMode.Update });
			await tryGetTask.ConfigureAwait(false);
			var tryGetResult = tryGetTask.GetPropertyValue<object>("Result");

			// Only update the value if it exists.
			if (!tryGetResult.GetPropertyValue<bool>("HasValue"))
				return HttpStatusCode.NotFound;

			// Validate the ETag.
			var currentValue = tryGetResult.GetPropertyValue<object>("Value");
			var currentEtag = CRC64.ToCRC64(JsonConvert.SerializeObject(currentValue)).ToString();
			if (currentEtag != operation.Etag)
				return HttpStatusCode.PreconditionFailed;

			// Delete from reliable dictionary.
			var tryDeleteMethod = dictionaryType.GetMethod("TryRemoveAsync", new[] { typeof(ITransaction), keyType });
			await ((Task)tryDeleteMethod.Invoke(dictionary, new[] { tx, key })).ConfigureAwait(false);

			return HttpStatusCode.OK;
		}

		/// <summary>
		/// Get the queryable reliable collection by name.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <returns>The reliable collection that supports querying.</returns>
		private static async Task<IReliableState> GetQueryableState(this IReliableStateManager stateManager, string collection)
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

		/// <summary>
		/// Get the Entity model type from the reliable dictionary.
		/// This is the full metadata type definition for the rows in the
		/// dictionary (key, value, partition, etag).
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The Entity model type for the dictionary.</returns>
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
		/// Gets the values from the reliable state as the <see cref="Entity{TKey, TValue}"/> of the collection.
		/// </summary>
		/// <param name="state">Reliable state (must implement <see cref="IReliableDictionary{TKey, TValue}"/>).</param>
		/// <param name="tx">Transaction to create the enumerable under.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>Values from the reliable state as <see cref="Entity{TKey, TValue}"/> values.</returns>
		private static async Task<IAsyncEnumerable<object>> GetAsyncEnumerable(this IReliableState state,
			ITransaction tx, IReliableStateManager stateManager, Guid partitionId, CancellationToken cancellationToken)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			var entityType = state.GetEntityType();

			// Create the async enumerable.
			var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(state.GetType().GetGenericArguments());
			var createEnumerableAsyncTask = state.CallMethod<Task>("CreateEnumerableAsync", new[] { typeof(ITransaction) }, tx);
			await createEnumerableAsyncTask.ConfigureAwait(false);

			// Get the AsEntity method to convert to an Entity enumerable.
			var asyncEnumerable = createEnumerableAsyncTask.GetPropertyValue<object>("Result");
			var asEntityMethod = typeof(ReliableStateExtensions).GetMethod("AsEntity", BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(entityType.GenericTypeArguments);
			return (IAsyncEnumerable<object>)asEntityMethod.Invoke(null, new object[] { asyncEnumerable, partitionId, cancellationToken });
		}

		/// <summary>
		/// Lazily convert the reliable state enumerable into a queryable <see cref="Entity{TKey, TValue}"/> enumerable.
		/// /// </summary>
		/// <param name="source">Reliable state enumerable.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The</returns>
		private static IAsyncEnumerable<Entity<TKey, TValue>> AsEntity<TKey, TValue>(this IAsyncEnumerable<KeyValuePair<TKey, TValue>> source, Guid partitionId, CancellationToken cancellationToken)
		{
			return source.SelectAsync(kvp => new Entity<TKey, TValue>
			{
				PartitionId = partitionId,
				Key = kvp.Key,
				Value = kvp.Value,

				// TODO: only set the ETag if the query selects this object.
				Etag = CRC64.ToCRC64(JsonConvert.SerializeObject(kvp.Value)).ToString(),
			});
		}

		private static async Task<IEnumerable<Partition>> GetPartitionsAsync(StatefulServiceContext context)
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(context.ServiceName).ConfigureAwait(false);
				return partitions.Where(p => p.PartitionInformation.Id != context.PartitionId);
			}
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
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var result = options.ApplyTo(casted.AsQueryable(), settings);

			// Get the query results.
			return result.Cast<object>();
		}

		/// <summary>
		/// Apply the OData query specified by <paramref name="query"/> to the objects.
		/// </summary>
		/// <param name="data">The objects to query.</param>
		/// <param name="type">The Type of the objects in <paramref name="data"/>.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="aggregate">Indicates whether this is an aggregation or partial query.</param>
		/// <returns>The results of applying the query to the in-memory objects.</returns>
		private static IAsyncEnumerable<object> ApplyQuery(IAsyncEnumerable<object> data, Type type,
			IEnumerable<KeyValuePair<string, string>> query, bool aggregate)
		{
			// Get the OData query context for this type.
			var context = QueryCache.GetQueryContext(type);

			// Execute the query.
			var options = new ODataQueryOptions(query, context, aggregate);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			return options.ApplyTo(data, settings);
		}
	}
}