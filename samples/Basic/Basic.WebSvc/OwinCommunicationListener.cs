using System;
using System.Fabric;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Owin;

namespace Basic.WebSvc
{
	internal class OwinCommunicationListener : ICommunicationListener
	{
		private readonly Action<IAppBuilder> startup;
		private readonly ServiceContext serviceContext;
		private readonly string endpointName;
		private readonly string appRoot;

		private IDisposable webApp;
		private string publishAddress;
		private string listeningAddress;

		public OwinCommunicationListener(Action<IAppBuilder> startup, ServiceContext serviceContext, string endpointName)
			: this(startup, serviceContext, endpointName, null)
		{
		}

		public OwinCommunicationListener(Action<IAppBuilder> startup, ServiceContext serviceContext, string endpointName, string appRoot)
		{
			this.startup = startup ?? throw new ArgumentNullException(nameof(startup));
			this.serviceContext = serviceContext ?? throw new ArgumentNullException(nameof(serviceContext));
			this.endpointName = endpointName ?? throw new ArgumentNullException(nameof(endpointName));
			this.appRoot = appRoot;
		}

		public bool ListenOnSecondary { get; set; }

		public Task<string> OpenAsync(CancellationToken cancellationToken)
		{
			var serviceEndpoint = this.serviceContext.CodePackageActivationContext.GetEndpoint(this.endpointName);
			int port = serviceEndpoint.Port;

			if (this.serviceContext is StatefulServiceContext)
			{
				StatefulServiceContext statefulServiceContext = this.serviceContext as StatefulServiceContext;

				this.listeningAddress = string.Format(
					CultureInfo.InvariantCulture,
					"http://+:{0}/{1}{2}/{3}/{4}",
					port,
					string.IsNullOrWhiteSpace(this.appRoot)
						? string.Empty
						: this.appRoot.TrimEnd('/') + '/',
					statefulServiceContext.PartitionId,
					statefulServiceContext.ReplicaId,
					Guid.NewGuid());
			}
			else if (this.serviceContext is StatelessServiceContext)
			{
				this.listeningAddress = string.Format(
					CultureInfo.InvariantCulture,
					"http://+:{0}/{1}",
					port,
					string.IsNullOrWhiteSpace(this.appRoot)
						? string.Empty
						: this.appRoot.TrimEnd('/') + '/');
			}
			else
			{
				throw new InvalidOperationException();
			}

			this.publishAddress = this.listeningAddress.Replace("+", FabricRuntime.GetNodeContext().IPAddressOrFQDN);

			try
			{
				this.webApp = WebApp.Start(this.listeningAddress, appBuilder => this.startup.Invoke(appBuilder));

				return Task.FromResult(this.publishAddress);
			}
			catch (Exception)
			{
				this.StopWebServer();
				throw;
			}
		}

		public Task CloseAsync(CancellationToken cancellationToken)
		{
			this.StopWebServer();
			return Task.FromResult(true);
		}

		public void Abort()
		{
			this.StopWebServer();
		}

		private void StopWebServer()
		{
			if (this.webApp != null)
			{
				try
				{
					this.webApp.Dispose();
				}
				catch (ObjectDisposedException)
				{
					// no-op
				}
			}
		}
	}
}
