using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Queryable;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.UserSvc
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

		private Task InvokeQueryHandler(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
			var request = httpContext.Request;

			// Handle CORS.
			AddAccessControlHeaders(httpContext.Request, httpContext.Response);
			if (request.Method == HttpMethods.Options)
				return HandleCORS(httpContext);

			// GET query/$metadata
			if (request.Method == HttpMethods.Get && request.Path == "/query/$metadata")
				return GetMetadataAsync(httpContext, serviceContext, stateManager);

			var segments = request.Path.Value.Split(PathSplit, StringSplitOptions.RemoveEmptyEntries);

			// GET query/<collection-name>
			if (request.Method == HttpMethods.Get && segments.Length == 2)
				return QueryCollectionAsync(httpContext, serviceContext, stateManager, segments[1]);

			// GET query/<collection-name>/<partition-id>
			if (request.Method == HttpMethods.Get && segments.Length == 3 && Guid.TryParse(segments[2], out Guid partitionId))
				return QueryCollectionAsync(httpContext, serviceContext, stateManager, segments[1], partitionId);

			// POST query
			if (request.Method == HttpMethods.Post && segments.Length == 1)
				return UpdateCollectionAsync(httpContext, serviceContext, stateManager);

			// POST query/<partition-id>
			if (request.Method == HttpMethods.Post && segments.Length == 2 && Guid.TryParse(segments[1], out partitionId))
				return UpdateCollectionAsync(httpContext, serviceContext, stateManager, partitionId);

			// Unknown queryable method.
			return NotFound(httpContext);
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
			// Query the reliable collection.

			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			await httpContext.Response.WriteAsync("{}").ConfigureAwait(false);
		}

		private async Task QueryCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager, string collection, Guid partitionId)
		{
			// Query the reliable collection.
			var query = httpContext.Request.Query.Select(p => new KeyValuePair<string, string>(p.Key, p.Value));
			var results = await stateManager.QueryPartitionAsync(collection, query, partitionId, CancellationToken.None).ConfigureAwait(false);

			httpContext.Response.ContentType = "application/json";
			httpContext.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			string response = JsonConvert.SerializeObject(results.Select(r => JsonConvert.DeserializeObject<JObject>(r)));
			await httpContext.Response.WriteAsync(response).ConfigureAwait(false);
		}

		private async Task UpdateCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager)
		{
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

		private async Task UpdateCollectionAsync(HttpContext httpContext, StatefulServiceContext serviceContext, IReliableStateManager stateManager, Guid partitionId)
		{
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
	}

	public static class ODataQueryableMiddlewareExtensions
	{
		public static IApplicationBuilder UseODataQueryable(this IApplicationBuilder app)
		{
			return app.UseMiddleware<ODataQueryableMiddleware>();
		}
	}
}
