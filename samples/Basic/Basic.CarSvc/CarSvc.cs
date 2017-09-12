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

namespace Basic.CarSvc
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class CarSvc : StatefulService
	{
		public CarSvc(StatefulServiceContext context)
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

		protected override async Task RunAsync(CancellationToken cancellationToken)
		{
			var cars = await StateManager.GetOrAddAsync<IReliableDictionary<string, Car>>("cars");

			for (int i = 0; i < 50; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var car = new Car
					{
						VIN = $"JTK{i}",
						Make = i % 3 == 0 ? "Ford" : i % 3 == 1 ? "Dodge" : "Toyota",
						Model = i % 6 == 0 ? "Mustang" : i % 6 == 1 ? "Challenger" : i % 6 == 2 ? "Prius" : i % 6 == 3 ? "Explorer" : i % 6 == 4 ? "Charger" : "Fortuner",
						Year = 2000 + (i % 10),
						Price = 20000 + i,
						MPG = 30 + (i % 6 == 2 ? 20 : 3),
					};

					await cars.SetAsync(tx, car.VIN, car, TimeSpan.FromSeconds(4), cancellationToken);
					await tx.CommitAsync();
				}
			}
		}
	}
}
