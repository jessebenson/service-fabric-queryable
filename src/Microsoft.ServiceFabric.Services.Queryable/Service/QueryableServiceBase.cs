using Microsoft.Data.Edm;
using Microsoft.Data.OData;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public abstract class QueryableServiceBase : StatefulServiceBase, IQueryableService
	{
		private readonly QueryModelCache QueryCache = new QueryModelCache();
		private readonly IReliableStateManager StateManager;

		public QueryableServiceBase(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
			StateManager = reliableStateManagerReplica;
		}

		async Task<string> IQueryableService.GetMetadataAsync()
		{
			ODataModelBuilder builder = new ODataConventionModelBuilder();
			foreach (var queryable in await StateManager.GetQueryableTypes().ConfigureAwait(false))
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

		async Task<IEnumerable<string>> IQueryableService.QueryAsync(string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			// Find the reliable state.
			var reliableStateResult = await StateManager.TryGetAsync<IReliableState>(collection).ConfigureAwait(false);
			if (!reliableStateResult.HasValue)
				throw new ArgumentException($"IReliableState '{collection}' not found.");

			// Verify the state is a reliable dictionary.
			var reliableState = reliableStateResult.Value;
			if (!reliableState.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException($"IReliableState '{collection}' must be an IReliableDictionary.");

			// Get the data from the reliable state.
			var results = await reliableState.ToEnumerable(StateManager).ConfigureAwait(false);

			// Filter the data.
			if (query.Any())
			{
				var valueType = reliableState.GetValueType();
				results = ApplyQuery(results, valueType, query);
			}

			// Return the filtered data as json.
			return results.Select(JsonConvert.SerializeObject);
		}

		private IEnumerable<object> ApplyQuery(IEnumerable<object> data, Type type, IEnumerable<KeyValuePair<string, string>> filter)
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
