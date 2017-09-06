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
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Results;
using System.Xml;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	/// <remarks>
	/// DEPRECATED: this class will be removed in favor of HTTP middleware.
	/// </remarks>
	public abstract class QueryableController : ApiController
	{
		protected async Task<IHttpActionResult> GetMetadataAsync(string application, string service)
		{
			var serviceUri = GetServiceUri(application, service);

			try
			{
				string content = string.Empty;
				if (Request.Method == HttpMethod.Get)
				{
					var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri).ConfigureAwait(false);
					var metadata = await proxy.GetMetadataAsync().ConfigureAwait(false);

					// Parse the metadata as xml.
					XmlDocument xml = new XmlDocument();
					xml.LoadXml(metadata);

					// Return xml response.
					content = xml.InnerXml;
				}

				// Return response, with appropriate CORS headers.
				var response = new HttpResponseMessage { Content = new StringContent(content, Encoding.UTF8, "application/xml") };
				AddAccessControlHeaders(Request, response);

				return new ResponseMessageResult(response);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		protected async Task<IHttpActionResult> GetPartitionMetadataAsync(string application, string service, Guid partitionId)
		{
			var serviceUri = GetServiceUri(application, service);

			try
			{
				string content = string.Empty;
				if (Request.Method == HttpMethod.Get)
				{
					var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri, partitionId).ConfigureAwait(false);
					var metadata = await proxy.GetMetadataAsync().ConfigureAwait(false);

					// Parse the metadata as xml.
					XmlDocument xml = new XmlDocument();
					xml.LoadXml(metadata);

					// Return xml response.
					content = xml.InnerXml;
				}

				// Return response, with appropriate CORS headers.
				var response = new HttpResponseMessage { Content = new StringContent(content, Encoding.UTF8, "application/xml") };
				AddAccessControlHeaders(Request, response);

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
				string content = string.Empty;
				if (Request.Method == HttpMethod.Get)
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

					// Return json response.
					content = JsonConvert.SerializeObject(result);
				}

				// Return response, with appropriate CORS headers.
				var response = new HttpResponseMessage { Content = new StringContent(content, Encoding.UTF8, "application/json") };
				AddAccessControlHeaders(Request, response);

				return new ResponseMessageResult(response);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		protected async Task<IHttpActionResult> QuerySpecificPartitionAsync(string application, string service, Guid partitionId, string collection)
		{
			var serviceUri = GetServiceUri(application, service);

			try
			{
				string content = string.Empty;
				if (Request.Method == HttpMethod.Get)
				{
					var query = Request.GetQueryNameValuePairs();

					// Query one specific service partition.
					var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri, partitionId).ConfigureAwait(false);
					var results = await proxy.QueryPartitionAsync(collection, query);

					// Construct the final, aggregated result.
					var result = new ODataResult
					{
						ODataMetadata = "",
						Value = results.Select(JsonConvert.DeserializeObject<JObject>),
					};

					// Return json response.
					content = JsonConvert.SerializeObject(result);
				}

				// Return response, with appropriate CORS headers.
				var response = new HttpResponseMessage { Content = new StringContent(content, Encoding.UTF8, "application/json") };
				AddAccessControlHeaders(Request, response);

				return new ResponseMessageResult(response);
			}
			catch (Exception e)
			{
				return HandleException(e, serviceUri);
			}
		}

		private void AddAccessControlHeaders(HttpRequestMessage request, HttpResponseMessage response)
		{
			response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");

			IEnumerable<string> headers;
			if (request.Headers.TryGetValues("Origin", out headers))
				response.Headers.Add("Access-Control-Allow-Origin", headers);
			if (request.Headers.TryGetValues("Access-Control-Request-Headers", out headers))
				response.Headers.Add("Access-Control-Allow-Headers", headers);
		}

		protected async Task<IHttpActionResult> ExecuteAsync(string application, string service, EntityOperation<JToken, JToken>[] operations)
		{
			var serviceUri = GetServiceUri(application, service);
			try
			{
				string content = string.Empty;
				if (Request.Method == HttpMethod.Post)
				{
					Dictionary<Guid, List<int>> preMap = new Dictionary<Guid, List<int>>();
					List<DmlResult> finalResult = new List<DmlResult>();
					
					for (int i = 0; i < operations.Length; i++)
					{
						List<int> templist = new List<int>();
						if (operations[i].PartitionId == Guid.Empty)
						{
							operations[i].PartitionId = await GetRandomPartitionId(serviceUri);
						}
						if (preMap.ContainsKey(operations[i].PartitionId))
						{
							templist = preMap[operations[i].PartitionId];
							templist.Add(i);
							preMap[operations[i].PartitionId] = templist;
						}
						else
						{
							templist.Add(i);
							preMap[operations[i].PartitionId] = templist;
						}
					}

					int p = 0;
					foreach (Guid mypid in preMap.Keys)
					{
						//Fetch partition proxy.
						var proxy = await GetServiceProxyAsync<IQueryableService>(serviceUri, mypid).ConfigureAwait(false);
						var backendOperations = new List<EntityOperation<string, string>>();

						var listOfStatusCodes = new List<int>();
						foreach (int myref in preMap[mypid])
						{
							var op = operations[myref];
							var backendOperation = new EntityOperation<string, string>
							{
								Operation = op.Operation,
								Collection = op.Collection,
								PartitionId = op.PartitionId,
								Key = JsonConvert.SerializeObject(op.Key, new JsonSerializerSettings { StringEscapeHandling = StringEscapeHandling.EscapeNonAscii }),
								Value = JsonConvert.SerializeObject(op.Value, new JsonSerializerSettings { StringEscapeHandling = StringEscapeHandling.EscapeNonAscii }),
								Etag = op.Etag,
							};

							backendOperations.Add(backendOperation);

							DmlResult tempResult = new DmlResult();
							tempResult.Key = operations[myref].Key;
							tempResult.Collection = operations[myref].Collection;
							tempResult.PartitionId = mypid;

							finalResult.Add(tempResult);
						}

						listOfStatusCodes = await proxy.ExecuteAsync(backendOperations.ToArray());

						foreach (var row in listOfStatusCodes)
						{
							if (p < finalResult.Count)
							{
								finalResult[p].Status = row; 
								p++;
							}
						}
					}

					content = JsonConvert.SerializeObject(finalResult);
				}

				// Return response, with appropriate CORS headers.
				var response = new HttpResponseMessage { Content = new StringContent(content, Encoding.UTF8, "application/json") };
				AddAccessControlHeaders(Request, response);

				return new ResponseMessageResult(response);
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

			if (e is HttpException)
				return Content((HttpStatusCode)((HttpException)e).GetHttpCode(), ((HttpException)e).Message);
			if (e.InnerException is HttpException)
				return Content((HttpStatusCode)((HttpException)e.InnerException).GetHttpCode(), ((HttpException)e.InnerException).Message);

			if (e is AggregateException)
				return InternalServerError(e.InnerException ?? e);

			return InternalServerError(e);
		}

		private static readonly ThreadLocal<Random> Random = new ThreadLocal<Random>(() => new Random());

		private static async Task<T> GetServiceProxyAsync<T>(Uri serviceUri) where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				int randomindex = Random.Value.Next(0, partitions.Count);
				return CreateServiceProxy<T>(serviceUri, partitions[randomindex]);
			}
		}

		private static async Task<T> GetServiceProxyAsync<T>(Uri serviceUri, Guid partitionId)
			where T : IService
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				var matchingPartition = partitions.FirstOrDefault(p => p.PartitionInformation.Id == partitionId);
				if (matchingPartition == null)
				{
					throw new HttpException($"Partition '{partitionId}' not found.");
				}

				return CreateServiceProxy<T>(serviceUri, matchingPartition);
			}
		}

		private static async Task<Guid> GetRandomPartitionId(Uri serviceUri)
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceUri).ConfigureAwait(false);
				int randomindex = Random.Value.Next(0, partitions.Count);
				return partitions[randomindex].PartitionInformation.Id;
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