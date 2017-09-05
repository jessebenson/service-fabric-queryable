using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Basic.Common;

namespace Basic.UserSvc
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class UserSvc : StatefulService
	{
		public UserSvc(StatefulServiceContext context)
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
			var users = await StateManager.GetOrAddAsync<IReliableDictionary<string, UserProfile>>("users");

			for (int i = 0; i < 50; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var user = new UserProfile
					{
						Name = $"User {i}",
						Email = $"user-{i}@example.com",
						Age = 20 + i / 3,
						Address = new Address
						{
							AddressLine1 = $"1{i} Main St.",
							City = "Seattle",
							State = "WA",
							Zipcode = 98117,
						},
					};

					await users.SetAsync(tx, user.Email, user, TimeSpan.FromSeconds(4), cancellationToken);
					await tx.CommitAsync();
				}
			}
		}
	}
}
