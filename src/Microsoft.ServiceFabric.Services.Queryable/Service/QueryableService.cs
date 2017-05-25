using Microsoft.ServiceFabric.Data;
using System.Fabric;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public abstract class QueryableService : QueryableServiceBase
	{
		public QueryableService(StatefulServiceContext serviceContext) : base(serviceContext, new ReliableStateManager(serviceContext))
		{
		}

		public QueryableService(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
		}
	}
}
