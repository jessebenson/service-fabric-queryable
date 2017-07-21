using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Results;
using System.Xml;
using Microsoft.ServiceFabric.Services.Queryable.Controller;



namespace Microsoft.ServiceFabric.Services.Queryable
{
	public abstract class QueryableController : ApiController
	{
		protected async Task<IHttpActionResult> GetMetadataAsync(string application, string service)
		{
			var serviceUri = GetServiceUri(application, service);

			try
			{
				var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri).ConfigureAwait(false);
				var metadata = await proxy.GetMetadataAsync().ConfigureAwait(false);

				// Parse the metadata as xml.
				XmlDocument xml = new XmlDocument();
				xml.LoadXml(metadata);

				// Return xml response.
				var response =
					new HttpResponseMessage {Content = new StringContent(xml.InnerXml, Encoding.UTF8, "application/xml")};
				return new ResponseMessageResult(response);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		protected async Task<IHttpActionResult> QueryAsync(string application, string service, string collection)
		{
			var serviceUri = GetServiceUri(application, service);

			try
			{
				var query = Request.GetQueryNameValuePairs();

				// Query one service partition, allowing the partition to do the distributed query.
				var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri).ConfigureAwait(false);
				var results = await proxy.QueryAsync(collection, query).ConfigureAwait(false);

				// Construct the final, aggregated result.
				var result = new ODataResult
				{
					ODataMetadata = "",
					Value = results.Select(JsonConvert.DeserializeObject<JObject>),
				};

				return Ok(result);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		protected async Task<IHttpActionResult> DeleteAsync(string application, string service, string collection,
			ValueViewModel[] obj)
		{
			var serviceUri = GetServiceUri(application, service);
			try
			{

				bool[][] results = new bool[obj.Length][];
				Dictionary<JToken, bool[]> results1 = new Dictionary<JToken, bool[]>();
				for (int i = 0; i < obj.Length; i++)
				{
					//Serialize the key from the json body and put it into a string.
					string keyquoted = JsonConvert.SerializeObject(obj[i].Key,
						new JsonSerializerSettings {StringEscapeHandling = StringEscapeHandling.EscapeNonAscii});

					//Fetch partition proxy.
					var proxy = await GetServiceProxyForPartitionAsync<IQueryableService>(serviceUri, obj[i].PartitionId)
						.ConfigureAwait(false);
					//Perform delete opration.
					results[i] = await Task.WhenAll(proxy.Select(p => p.DeleteAsync(collection, keyquoted)))
						.ConfigureAwait(false);
					results1[obj[i].Key] = results[i];
				}
				return Ok(results1);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}


		protected async Task<IHttpActionResult> AddAsync(string application, string service, string collection,
			ValueViewModel[] obj)
		{
			var serviceUri = GetServiceUri(application, service);
			try
			{
				bool[] results = new bool[obj.Length];
				for (int i = 0; i < obj.Length; i++)
				{
					//Serialize the key from the json body and put it into a string.
					string keyquoted = JsonConvert.SerializeObject(obj[i].Key,
						new JsonSerializerSettings {StringEscapeHandling = StringEscapeHandling.EscapeNonAscii});
					//Serialize the value from the json body and put it into a string.
					string valuequoted = JsonConvert.SerializeObject(obj[i].Value,
						new JsonSerializerSettings {StringEscapeHandling = StringEscapeHandling.EscapeNonAscii});
					//Fetch the proxy
					var proxy = await GetServiceProxyForAddAsync<IQueryableService>(serviceUri, obj[i].PartitionId)
						.ConfigureAwait(false);

					results[i] = await proxy.AddAsync(collection, keyquoted, valuequoted).ConfigureAwait(false);

				}
				return Ok(results);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}

		}

		protected async Task<IHttpActionResult> UpdateAsync(string application, string service, string collection,
			ValueViewModel[] obj)
		{
			var serviceUri = GetServiceUri(application, service);
			try
			{

				bool[][] results = new bool[obj.Length][];
				for (int i = 0; i < obj.Length; i++)
				{
					//Serialize the key from the json body and put it into a string.
					string keyquoted = JsonConvert.SerializeObject(obj[i].Key,
						new JsonSerializerSettings {StringEscapeHandling = StringEscapeHandling.EscapeNonAscii});
					//Serialize the value from the json body and put it into a string.
					string valuequoted = JsonConvert.SerializeObject(obj[i].Value,
						new JsonSerializerSettings {StringEscapeHandling = StringEscapeHandling.EscapeNonAscii});

					var proxy = await GetServiceProxyForPartitionAsync<IQueryableService>(serviceUri, obj[i].PartitionId)
						.ConfigureAwait(false);
					results[i] = await Task.WhenAll(proxy.Select(p => p.UpdateAsync(collection, keyquoted, valuequoted)))
						.ConfigureAwait(false);
				}
				return Ok(results);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}

		}

		private IHttpActionResult HandleException(Exception e, Uri serviceUri)
		{
			if (e is FabricServiceNotFoundException)
				return Content(HttpStatusCode.NotFound, new {Message = $"Service '{serviceUri}' not found."});

			if (e is ArgumentException)
				return BadRequest(e.Message);
			if (e.InnerException is ArgumentException)
				return BadRequest(e.InnerException.Message);
			if (e is HttpException)
				return Content((HttpStatusCode) ((HttpException) e).GetHttpCode(), ((HttpException) e).Message);
			if (e.InnerException is HttpException)
				return Content((HttpStatusCode) ((HttpException) e.InnerException).GetHttpCode(),
					((HttpException) e.InnerException).Message);

			if (e is AggregateException)
				return InternalServerError(e.InnerException ?? e);

			return InternalServerError(e);
		}

		private static async Task<T> GetServiceProxyAsync<T>(Uri serviceUri) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				return CreateServiceProxy<T>(serviceUri, partitions.First());
			}
		}

		private static async Task<IEnumerable<T>> GetServiceProxyForPartitionAsync<T>(Uri serviceUri, Guid partitionId)
			where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				var matchingPartitions =
					partitions.Where(p => p.PartitionInformation.Id == partitionId || partitionId == Guid.Empty);
				return matchingPartitions.Select(p => CreateServiceProxy<T>(serviceUri, p));
			}
		}

		private static async Task<T> GetServiceProxyForAddAsync<T>(Uri serviceUri, Guid partitionId) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				var matchingPartitions = partitions.Where(p => p.PartitionInformation.Id == partitionId);
				if (partitionId == Guid.Empty)
				{
					return CreateServiceProxy<T>(serviceUri, partitions.First());
				}
				return CreateServiceProxy<T>(serviceUri, matchingPartitions.First());

			}
		}

		private static T CreateServiceProxy<T>(Uri serviceUri, Partition partition) where T : IService
		{
			if (partition.PartitionInformation is Int64RangePartitionInformation)
				return ServiceProxy.Create<T>(serviceUri,
					new ServicePartitionKey(((Int64RangePartitionInformation) partition.PartitionInformation).LowKey));
			if (partition.PartitionInformation is NamedPartitionInformation)
				return ServiceProxy.Create<T>(serviceUri,
					new ServicePartitionKey(((NamedPartitionInformation) partition.PartitionInformation).Name));
			if (partition.PartitionInformation is SingletonPartitionInformation)
				return ServiceProxy.Create<T>(serviceUri);

			throw new ArgumentException(nameof(partition));
		}

		private static Uri GetServiceUri(string applicationName, string serviceName)
		{
			var applicationUri = new Uri($"fabric:/{applicationName}/");
			return new Uri(applicationUri, serviceName);
		}
	}
}
