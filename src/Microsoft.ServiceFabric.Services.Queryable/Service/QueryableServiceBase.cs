using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	/// <remarks>
	/// DEPRECATED: this class will be removed in favor of HTTP middleware.
	/// </remarks>
	public abstract class QueryableServiceBase : StatefulServiceBase, IQueryableService
	{
		private readonly IReliableStateManager StateManager;

		protected QueryableServiceBase(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
			StateManager = reliableStateManagerReplica;
		}

		Task<string> IQueryableService.GetMetadataAsync()
		{
			return StateManager.GetMetadataAsync();
		}

		Task<IEnumerable<string>> IQueryableService.QueryAsync(string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			return StateManager.QueryAsync(Context, collection, query, CancellationToken.None);
		}

		Task<IEnumerable<string>> IQueryableService.QueryPartitionAsync(string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			return StateManager.QueryPartitionAsync(collection, query, this.Partition.PartitionInfo.Id, CancellationToken.None);
		}

		Task<List<int>> IQueryableService.ExecuteAsync(EntityOperation<string, string>[] operations)
		{
			return StateManager.ExecuteAsync(operations.Select(o => new EntityOperation<JToken, JToken>
			{
				Operation = o.Operation,
				Collection = o.Collection,
				PartitionId = o.PartitionId,
				Key = JsonConvert.DeserializeObject<JToken>(o.Key),
				Value = JsonConvert.DeserializeObject<JToken>(o.Value),
				Etag = o.Etag,
			}).ToArray());
		}
	}
}