using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Services.Queryable;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Basic.Common;

namespace Basic.ProductSvc
{
	/// <summary>
	/// An instance of this class is created for each service replica by the Service Fabric runtime.
	/// </summary>
	internal sealed class ProductSvc : QueryableService
	{
		public ProductSvc(StatefulServiceContext context)
			: base(context)
		{ }

		/// <summary>
		/// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
		/// </summary>
		/// <remarks>
		/// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
		/// </remarks>
		/// <returns>A collection of listeners.</returns>
		protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
		{
			return new[]
			{
				new ServiceReplicaListener(this.CreateServiceRemotingListener),
			};
		}

		/// <summary>
		/// This is the main entry point for your service replica.
		/// This method executes when this replica of your service becomes primary and has write status.
		/// </summary>
		/// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
		protected override async Task RunAsync(CancellationToken cancellationToken)
		{
			var products = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Product>>("products");

			// Add some initial products.
			int partitionIndex = await GetPartitionIndex().ConfigureAwait(false);
			for (int i = 0; i < 10; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var key = $"sku-{partitionIndex}-{i}";
					var value = new Product { Sku = key, Price = 10.0 + (i / 10.0), Quantity = i };

					await products.SetAsync(tx, key, value, TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);
					await tx.CommitAsync().ConfigureAwait(false);
				}
			}
		}

		private async Task<int> GetPartitionIndex()
		{
			using (var client = new FabricClient())
			{
				var partitionList = await client.QueryManager.GetPartitionListAsync(Context.ServiceName).ConfigureAwait(false);
				var partitions = partitionList.Select(p => p.PartitionInformation.Id).OrderBy(id => id).ToList();
				return partitions.IndexOf(Context.PartitionId);
			}
		}
	}
}
