﻿using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public abstract class QueryableService : StatefulService, IQueryableService
	{
		public QueryableService(StatefulServiceContext serviceContext) : base(serviceContext)
		{
		}

		public QueryableService(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
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

    }
}
