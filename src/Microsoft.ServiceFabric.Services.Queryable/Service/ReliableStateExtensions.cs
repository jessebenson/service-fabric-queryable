using Microsoft.Data.Edm;
using Microsoft.Data.OData;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

		public static async Task<IEnumerable<KeyValuePair<string, Type>>> GetQueryableTypes(this IReliableStateManager stateManager, CancellationToken cancellationToken = default(CancellationToken))
		{
			var types = new Dictionary<string, Type>();
			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
			{
				var state = enumerator.Current;
				if (state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				{
					types.Add(state.Name.AbsolutePath, state.GetValueType());
				}
			}

			return types;
		}

		public static async Task<string> GetMetadataAsync(this IReliableStateManager stateManager)
		{
			var builder = new ODataConventionModelBuilder();
			foreach (var queryable in await stateManager.GetQueryableTypes().ConfigureAwait(false))
			{
				var entity = builder.AddEntity(queryable.Value);
				builder.AddEntitySet(queryable.Key, entity);
			}

			IEdmModel model = builder.GetEdmModel();

			using (var stream = new MemoryStream())
			using (var message = new InMemoryMessage { Stream = stream })
			{
				var settings = new ODataMessageWriterSettings();
				ODataMessageWriter writer = new ODataMessageWriter((IODataResponseMessage)message, settings, model);
				writer.WriteMetadataDocument();
				return Encoding.UTF8.GetString(stream.ToArray());
			}
		}

		public static async Task<IEnumerable<string>> QueryAsync(this IReliableStateManager stateManager, string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			// Find the reliable state.
			var reliableStateResult = await stateManager.TryGetAsync<IReliableState>(collection).ConfigureAwait(false);
			if (!reliableStateResult.HasValue)
				throw new ArgumentException($"IReliableState '{collection}' not found.");

			// Verify the state is a reliable dictionary.
			var reliableState = reliableStateResult.Value;
			if (!reliableState.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException($"IReliableState '{collection}' must be an IReliableDictionary.");

			// Get the data from the reliable state.
			var results = await stateManager.GetEnumerable(reliableState).ConfigureAwait(false);

			// Filter the data.
			if (query.Any())
			{
				var valueType = reliableState.GetValueType();
				results = ApplyQuery(results, valueType, query);
			}

			// Return the filtered data as json.
			return results.Select(JsonConvert.SerializeObject);
		}

		/// <summary>
		/// Reads all values from the reliable state into an in-memory collection.
		/// </summary>
		/// <param name="state">Reliable state (must implement <see cref="IReliableDictionary{TKey, TValue}"/>).</param>
		/// <param name="stateManager">Reliable state manager, to create a transaction.</param>
		/// <returns>All values from the reliable state in an in-memory collection.</returns>
		private static async Task<IEnumerable<object>> GetEnumerable(this IReliableStateManager stateManager, IReliableState state)
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

		private static IEnumerable<object> ApplyQuery(IEnumerable<object> data, Type type, IEnumerable<KeyValuePair<string, string>> filter)
		{
			// Get the OData query context for this type.
			var context = QueryCache.GetQueryContext(type);

			// Cast to correct IQueryable type.
			var casted = data.CastEnumerable(type);

			// Execute the query.
			var oDataQueryOptions = new ODataQueryOptions(filter, context);
			var result = oDataQueryOptions.ApplyTo(casted.AsQueryable(), new ODataQuerySettings());

			// Get the query results.
			return result.ToEnumerable();
		}
	}
}
