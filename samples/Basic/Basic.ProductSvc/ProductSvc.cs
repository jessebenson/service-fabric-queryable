using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Basic.Common;

namespace Basic.ProductSvc
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class ProductSvc : StatefulService
	{
		public ProductSvc(StatefulServiceContext context)
			: base(context)
		{ }

		/// <summary>
		/// Optional override to create listeners (like tcp, http) for this service instance.
		/// </summary>
		/// <returns>The collection of listeners.</returns>
		protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
		{
			return new ServiceReplicaListener[]
			{
				new ServiceReplicaListener(serviceContext =>
					new KestrelCommunicationListener(serviceContext, (url, listener) =>
					{
						return new WebHostBuilder()
							.UseKestrel()
							.ConfigureServices(
								services => services
									.AddSingleton<StatefulServiceContext>(serviceContext)
									.AddSingleton<IReliableStateManager>(this.StateManager))
							.UseContentRoot(Directory.GetCurrentDirectory())
							.UseStartup<Startup>()
							.UseApplicationInsights()
							.UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseUniqueServiceUrl)
							.UseUrls(url)
							.Build();
					}))
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
			var cars = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Cars>>("Cars");

			int partitionIndex = await GetPartitionIndex().ConfigureAwait(false);
			for (int i = 0; i < 10; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var key = $"sku-{i}";
					var product = new Product { Sku = key, Price = 10.0 + (i / 10.0), Quantity = i };
					var car = new Cars { Model = key, Price = 8000 + (i / 10.0), HorsePower = i * 50, MPG = i + 30 };
					await products.SetAsync(tx, key, product, TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);
					await cars.SetAsync(tx, key, car, TimeSpan.FromSeconds(4), cancellationToken).ConfigureAwait(false);

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
