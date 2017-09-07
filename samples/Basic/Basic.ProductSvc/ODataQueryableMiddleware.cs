using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.ServiceFabric.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public sealed class ODataQueryableMiddleware
	{
		private static readonly char[] PathSplit = new[] { '/' };

		private readonly RequestDelegate _next;

		public ODataQueryableMiddleware(RequestDelegate next)
		{
			_next = next;
		}

		public Task Invoke(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
			// Queryable handlers.
			if (httpContext.Request.Path.StartsWithSegments("/query"))
			{
				return InvokeQueryHandler(httpContext, serviceContext, stateManager);
			}

			return _next.Invoke(httpContext);
		}

		private async Task InvokeQueryHandler(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
			try
			{
				var request = httpContext.Request;
				var segments = request.Path.Value.Split(PathSplit, StringSplitOptions.RemoveEmptyEntries);

				AddAccessControlHeaders(httpContext.Request, httpContext.Response);

				if (request.Method == HttpMethods.Options)
				{
					// Handle CORS.
					await HandleCORS(httpContext).ConfigureAwait(false);
				}
				else if (request.Method == HttpMethods.Get && request.Path == "/query/$metadata")
				{
					// GET query/$metadata
					await GetMetadataAsync(httpContext, serviceContext, stateManager).ConfigureAwait(false);
				}
				else if (request.Method == HttpMethods.Get && segments.Length == 2)
				{
					// GET query/<collection-name>
					await QueryCollectionAsync(httpContext, serviceContext, stateManager, segments[1]).ConfigureAwait(false);
				}
				else if (request.Method == HttpMethods.Get && segments.Length == 3 && Guid.TryParse(segments[1], out Guid partitionId))
				{
					// GET query/<partition-id>/<collection-name>
					await QueryCollectionAsync(httpContext, serviceContext, stateManager, segments[2], partitionId).ConfigureAwait(false);
				}
				else if (request.Method == HttpMethods.Post && segments.Length == 1)
				{
					// POST query
					await ExecuteAsync(httpContext, serviceContext, stateManager).ConfigureAwait(false);
				}
				else
				{
					// Unknown queryable method.
					await NotFound(httpContext).ConfigureAwait(false);
				}
			}
			catch (Exception e)
			{
				await HandleException(httpContext, e).ConfigureAwait(false);
			}
		}

		private async Task GetMetadataAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
			// Query for metadata about reliable collections.
			string metadata = await stateManager.GetMetadataAsync().ConfigureAwait(false);

			httpContext.Response.ContentType = "application/xml";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			await httpContext.Response.WriteAsync(metadata).ConfigureAwait(false);
		}

		private async Task QueryCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager, string collection)
		{
			// Query the reliable collection for all partitions.
			var query = httpContext.Request.Query.Select(p => new KeyValuePair<string, string>(p.Key, p.Value));
			var results = await stateManager.QueryAsync(serviceContext, collection, query, CancellationToken.None).ConfigureAwait(false);

			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			var result = new ODataResult
			{
				ODataMetadata = "",
				Value = results,
			};
			string response = JsonConvert.SerializeObject(result);
			await httpContext.Response.WriteAsync(response).ConfigureAwait(false);
		}

		private async Task QueryCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager, string collection, Guid partitionId)
		{
			if (partitionId != serviceContext.PartitionId)
			{
				// Query this partition.
				await ForwardQueryCollectionAsync(httpContext, serviceContext, stateManager, collection, partitionId).ConfigureAwait(false);
				return;
			}

			// Query the local reliable collection.
			var query = httpContext.Request.Query.Select(p => new KeyValuePair<string, string>(p.Key, p.Value));
			var results = await stateManager.QueryPartitionAsync(collection, query, partitionId, CancellationToken.None).ConfigureAwait(false);

			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			var result = new ODataResult
			{
				ODataMetadata = "",
				Value = results,
			};
			string response = JsonConvert.SerializeObject(result);
			await httpContext.Response.WriteAsync(response).ConfigureAwait(false);
		}

		private async Task ForwardQueryCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager, string collection, Guid partitionId)
		{
			// Forward the request to the correct partition.
			var partition = await GetPartitionAsync(serviceContext, partitionId).ConfigureAwait(false);
			if (partition == null)
			{
				await NotFound(httpContext).ConfigureAwait(false);
				return;
			}

			using (var client = new HttpClient { BaseAddress = new Uri("http://localhost:19081/") })
			{
				string requestUri = $"{serviceContext.ServiceName.AbsolutePath}/query/{partitionId}/{collection}?{GetQueryParameters(httpContext, partition)}";
				var response = await client.GetAsync(requestUri).ConfigureAwait(false);
				var content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

				httpContext.Response.ContentType = response.Content.Headers.ContentType.MediaType;
				httpContext.Response.StatusCode = (int)response.StatusCode;

				// Write the response.
				await httpContext.Response.WriteAsync(content).ConfigureAwait(false);
			}
		}

		private async Task ExecuteAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
			// Read the body.
			var reader = new StreamReader(httpContext.Request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4096, leaveOpen: true);
			string content = await reader.ReadToEndAsync().ConfigureAwait(false);
			var operations = JsonConvert.DeserializeObject<EntityOperation<JToken, JToken>[]>(content);

			// Update the reliable collections.
			var results = await stateManager.ExecuteAsync(operations).ConfigureAwait(false);

			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			var response = JsonConvert.SerializeObject(results);
			await httpContext.Response.WriteAsync(response).ConfigureAwait(false);
		}

		private Task NotFound(HttpContext httpContext)
		{
			httpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
			httpContext.Response.Headers.Add("X-ServiceFabric", "ResourceNotFound");
			return Task.CompletedTask;
		}

		private Task BadRequest(HttpContext httpContext, string message)
		{
			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
			return httpContext.Response.WriteAsync(JsonConvert.SerializeObject(message));
		}

		private Task InternalServerError(HttpContext httpContext, string message)
		{
			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
			return httpContext.Response.WriteAsync(JsonConvert.SerializeObject(message));
		}

		private Task HandleException(HttpContext httpContext, Exception e)
		{
			if (e is ArgumentException)
				return BadRequest(httpContext, e.Message);
			if (e.InnerException is ArgumentException)
				return BadRequest(httpContext, e.InnerException.Message);

			//if (e is HttpException)
			//	return Content((HttpStatusCode)((HttpException)e).GetHttpCode(), ((HttpException)e).Message);
			//if (e.InnerException is HttpException)
			//	return Content((HttpStatusCode)((HttpException)e.InnerException).GetHttpCode(), ((HttpException)e.InnerException).Message);

			if (e is AggregateException)
				return InternalServerError(httpContext, (e.InnerException ?? e).Message);

			return InternalServerError(httpContext, e.Message);
		}

		private Task HandleCORS(HttpContext httpContext)
		{
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			return Task.CompletedTask;
		}

		private void AddAccessControlHeaders(HttpRequest request, HttpResponse response)
		{
			response.Headers.Add("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");

			StringValues headers;
			if (request.Headers.TryGetValue("Origin", out headers))
				response.Headers.Add("Access-Control-Allow-Origin", headers);
			if (request.Headers.TryGetValue("Access-Control-Request-Headers", out headers))
				response.Headers.Add("Access-Control-Allow-Headers", headers);
		}

		private string GetQueryParameters(HttpContext httpContext, Partition partition)
		{
			var partitionParameters = GetPartitionQueryParameters(partition);
			var queryParameters = partitionParameters.Concat(httpContext.Request.Query).Distinct();
			return string.Join("&", queryParameters.Select(p => $"{p.Key}={p.Value}"));
		}

		private IEnumerable<KeyValuePair<string, StringValues>> GetPartitionQueryParameters(Partition partition)
		{
			var info = partition.PartitionInformation;
			yield return new KeyValuePair<string, StringValues>("PartitionKind", info.Kind.ToString());

			if (info.Kind == ServicePartitionKind.Int64Range)
				yield return new KeyValuePair<string, StringValues>("PartitionKey", (info as Int64RangePartitionInformation).LowKey.ToString());
			else if (info.Kind == ServicePartitionKind.Named)
				yield return new KeyValuePair<string, StringValues>("PartitionKey", (info as NamedPartitionInformation).Name);
		}

		private async Task<Partition> GetPartitionAsync(StatefulServiceContext serviceContext, Guid partitionId)
		{
			using (var client = new FabricClient())
			{
				var partitions = await client.QueryManager.GetPartitionListAsync(serviceContext.ServiceName).ConfigureAwait(false);
				return partitions.FirstOrDefault(p => p.PartitionInformation.Id == partitionId);
			}
		}
	}

	public static class ODataQueryableMiddlewareExtensions
	{
		public static IApplicationBuilder UseODataQueryable(this IApplicationBuilder app)
		{
			return app.UseMiddleware<ODataQueryableMiddleware>();
		}
	}
}
