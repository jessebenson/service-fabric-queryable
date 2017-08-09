using Basic.Common;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Queryable;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.ProductSvc
{
	/// <summary>
	/// An instance of this class is created for each service replica by the Service Fabric runtime.
	/// </summary>
	internal sealed class ProductSvc : QueryableService, IProductService
	{
		public ProductSvc(StatefulServiceContext context)
			: base(context)
		{
		}

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
			var products = await GetProductsStateAsync();
			var products2 = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Product>>("products2");
			// Add some initial products.
			int partitionIndex = await GetPartitionIndex().ConfigureAwait(false);
			for (int i = 0; i < 10; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var key = $"sku-{i}";
					var key2 = $"sku-{i}";
					var value = new Product { Sku = key, Price = 10.0 + (i / 10.0), Quantity = i };
					var value2 = new Product { Sku = key, Price = 30.0 + (i / 10.0), Quantity = i * 2 };
					await products.SetAsync(tx, key, value, TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);
					await products2.SetAsync(tx, key2, value2, TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);
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

		private Task<IReliableDictionary<string, Product>> GetProductsStateAsync()
		{
			return this.StateManager.GetOrAddAsync<IReliableDictionary<string, Product>>("products");
		}

		async Task<Product> IProductService.GetProductAsync(string sku)
		{
			var products = await GetProductsStateAsync();
			using (var tx = StateManager.CreateTransaction())
			{
				var result = await products.TryGetValueAsync(tx, sku).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
				return result.Value;
			}
		}

		async Task IProductService.UpdateProductAsync(Product product)
		{
			var products = await GetProductsStateAsync();
			using (var tx = StateManager.CreateTransaction())
			{
				await products.SetAsync(tx, product.Sku, product).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
			}
		}

		async Task<Product> IProductService.DeleteProductAsync(string sku)
		{
			var products = await GetProductsStateAsync();
			using (var tx = StateManager.CreateTransaction())
			{
				var result = await products.TryRemoveAsync(tx, sku).ConfigureAwait(false);
				await tx.CommitAsync().ConfigureAwait(false);
				return result.Value;
			}
		}
	}
}