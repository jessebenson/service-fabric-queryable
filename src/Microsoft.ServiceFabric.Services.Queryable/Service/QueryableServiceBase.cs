using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
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

		Task<int> IQueryableService.DeleteAsync(string collection, string key)
		{
			return StateManager.DeleteAsync(collection, key);
		}

		Task<int> IQueryableService.AddAsync(Controller.BackendViewModel[] backendObjects)
		{
			return StateManager.AddAsync(backendObjects);
		}

		Task<int> IQueryableService.UpdateAsync(string collection, string key, string val)
		{
			return StateManager.UpdateAsync(collection, key, val);
		}
	}
}