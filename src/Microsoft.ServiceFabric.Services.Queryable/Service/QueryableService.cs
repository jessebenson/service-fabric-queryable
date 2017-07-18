using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;


namespace Microsoft.ServiceFabric.Services.Queryable
{
	public abstract class QueryableService : StatefulService, IQueryableService
	{
		protected QueryableService(StatefulServiceContext serviceContext) : base(serviceContext)
		{
		}

		protected QueryableService(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
		}

		Task<string> IQueryableService.GetMetadataAsync()
		{
			return StateManager.GetMetadataAsync();
		}

		Task<IEnumerable<string>> IQueryableService.QueryAsync(string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			return StateManager.QueryAsync(Context, collection, query, CancellationToken.None);
            //this.Partition.PartitionInfo.Id
		}

		Task<IEnumerable<string>> IQueryableService.QueryPartitionAsync(string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			return StateManager.QueryPartitionAsync(collection, query, this.Partition.PartitionInfo.Id, CancellationToken.None);
		}


        Task<bool> IQueryableService.DeleteAsync(string collection, string key )
        {
            return StateManager.DeleteAsync(collection,key);
        }


	    Task<bool> IQueryableService.AddAsync(string collection, string key, string val)
	    {
	        return StateManager.AddAsync(collection, key,val);
	    }
	    Task<bool> IQueryableService.UpdateAsync(string collection, string key, string val)
	    {
	        return StateManager.UpdateAsync(collection, key, val);
	    }
    }
}
