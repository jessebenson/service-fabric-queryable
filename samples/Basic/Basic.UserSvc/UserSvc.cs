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
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using Microsoft.ServiceFabric.Services.Queryable.LINQ;

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
			var users = await StateManager.GetOrAddAsync<IReliableDictionary<UserName, UserProfile>>("users");
            var indexed_users = await StateManager.GetOrAddIndexedAsync<UserName, UserProfile>("indexed_users",
                 FilterableIndex<UserName, UserProfile, string>.CreateQueryableInstance("Email"),
                   FilterableIndex<UserName, UserProfile, int>.CreateQueryableInstance("Age"));

            for (int i = 0; i < 100; i++)
			{
				using (var tx = StateManager.CreateTransaction())
				{
					var user = new UserProfile
					{
						Name = new UserName
						{
							First = $"First{i}",
							Last = $"Last{i}",
						},
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

					await users.SetAsync(tx, user.Name, user, TimeSpan.FromSeconds(4), cancellationToken);
                    await indexed_users.SetAsync(tx, user.Name, user, TimeSpan.FromSeconds(4), cancellationToken);
                    await tx.CommitAsync();
				}
			}

            /* Example of LINQ querying on IReliableIndexedDictionary
             
               Sometimes you will want your application code to carry out queries against your RC as well, and LINQ is a great way to do so
               It is important to note that currently the query must return type TValue, this is a bug

               Also, when you write your query, make sure to put all your WHERE logic into a single WHERE statement
               since each statement carries its own context the indexing middleware cannot efficiently operate on disjoint statements,
               e.g. use  qdict.Where(x => x.Email == "user-0@example.com" && x.Age <= 20) instead of  
                         qdict.Where(x => x.Email == "user-0@example.com").Where(x => x.Age <= 20)
            */

            /* Remove comments to run Linq
            
            // Create LINQ-Queryable state of IndexedDictionary
            QueryableReliableIndexedDictionary<UserName, UserProfile, UserProfile> qdict = new QueryableReliableIndexedDictionary<UserName, UserProfile, UserProfile>(indexed_users, StateManager);

            // Create the same query two different ways
            var query = from UserProfile profile in qdict
                        where profile.Age >= 20 && profile.Email == "user-0@example.com" && profile.Age <= 20
                        select profile;

            var query2 = qdict.Where(x => x.Email == "user-0@example.com" && x.Age <= 20);

            // Execute the queries, add breakpoints here to see results
            foreach (UserProfile profile in query)
            {

            }
            foreach (UserProfile profile in query2)
            {

            }
            
            */
        }
    }
}
