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
using System.Web.Http;
using System.Web.Http.Results;
using System.Xml;

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
				var response = new HttpResponseMessage { Content = new StringContent(xml.InnerXml, Encoding.UTF8, "application/xml") };
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

				// Query all service partitions concurrently.
				var proxies = await GetServiceProxiesAsync<IQueryableService>(serviceUri).ConfigureAwait(false);
				var results = await Task.WhenAll(proxies.Select(p => p.QueryAsync(collection, query))).ConfigureAwait(false);

				// Construct the final, aggregated result.
				var result = new ODataResult
				{
					ODataMetadata = "",
					Value = GetQueryResult(results, query),
				};

				return Ok(result);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		private IHttpActionResult HandleException(Exception e, Uri serviceUri)
		{
			if (e is FabricServiceNotFoundException)
				return Content(HttpStatusCode.NotFound, new { Message = $"Service '{serviceUri}' not found." });

			if (e is ArgumentException)
				return BadRequest(e.Message);
			if (e.InnerException is ArgumentException)
				return BadRequest(e.InnerException.Message);

			if (e is AggregateException)
				return InternalServerError(e.InnerException ?? e);

			return InternalServerError(e);
		}

		private static IEnumerable<JObject> GetQueryResult(IEnumerable<string>[] results, IEnumerable<KeyValuePair<string, string>> query)
		{
			var result = results.SelectMany(r => r);

			// Each individual service will return up to $top items.  We have to limit the final result to a total of $top items.
			int? top = GetTop(query);
			if (top != null)
			{
				result = result.Take(top.Value);
			}

			return result.Select(r => JsonConvert.DeserializeObject<JObject>(r));
		}

		private static int? GetTop(IEnumerable<KeyValuePair<string, string>> query)
		{
			foreach (var q in query)
			{
				if (q.Key == "$top")
					return int.Parse(q.Value);
			}

			return null;
		}

		private static async Task<T> GetServiceProxyAsync<T>(Uri serviceUri) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				return CreateServiceProxy<T>(serviceUri, partitions.First());
			}
		}

		private static async Task<IEnumerable<T>> GetServiceProxiesAsync<T>(Uri serviceUri) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				return partitions.Select(p => CreateServiceProxy<T>(serviceUri, p));
			}
		}

		private static T CreateServiceProxy<T>(Uri serviceUri, Partition partition) where T : IService
		{
			if (partition.PartitionInformation is Int64RangePartitionInformation)
				return ServiceProxy.Create<T>(serviceUri, new ServicePartitionKey(((Int64RangePartitionInformation)partition.PartitionInformation).LowKey));
			if (partition.PartitionInformation is NamedPartitionInformation)
				return ServiceProxy.Create<T>(serviceUri, new ServicePartitionKey(((NamedPartitionInformation)partition.PartitionInformation).Name));
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
