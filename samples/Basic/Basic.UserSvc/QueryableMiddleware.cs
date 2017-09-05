using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Queryable;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Basic.UserSvc
{
	public sealed class QueryableMiddleware
	{
		private static readonly char[] PathSplit = new[] { '/' };

		private readonly RequestDelegate _next;

		public QueryableMiddleware(RequestDelegate next)
		{
			_next = next;
		}

		public Task Invoke(HttpContext context, IReliableStateManager stateManager)
		{
			// Queryable handlers.
			var request = context.Request;
			if (request.Path.StartsWithSegments("/query"))
			{
				// Handle CORS.
				AddAccessControlHeaders(request, context.Response);
				if (request.Method == HttpMethods.Options)
					return HandleCORS(context);

				// $metadata
				if (request.Method == HttpMethods.Get && request.Path == "/query/$metadata")
					return GetMetadataAsync(context, stateManager);

				// Query reliable collections.
				var segments = request.Path.Value.Split(PathSplit, StringSplitOptions.RemoveEmptyEntries);
				if (request.Method == HttpMethods.Get && segments.Length == 2)
					return QueryCollectionAsync(context, stateManager, segments[1]);
				if (request.Method == HttpMethods.Get && segments.Length == 3 && Guid.TryParse(segments[2], out Guid partitionId))
					return QueryCollectionAsync(context, stateManager, segments[1], partitionId);

				// Unknown queryable method.
				return NotFound(context);
			}

			return _next.Invoke(context);
		}

		private async Task GetMetadataAsync(HttpContext context, IReliableStateManager stateManager)
		{
			// Query for metadata about reliable collections.
			string metadata = await stateManager.GetMetadataAsync().ConfigureAwait(false);

			context.Response.ContentType = "application/xml";
			context.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			await context.Response.WriteAsync(metadata).ConfigureAwait(false);
		}

		private async Task QueryCollectionAsync(HttpContext context, IReliableStateManager stateManager, string collection)
		{
			// Query the reliable collection.

			context.Response.ContentType = "application/json";
			context.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			await context.Response.WriteAsync("{}").ConfigureAwait(false);
		}

		private async Task QueryCollectionAsync(HttpContext context, IReliableStateManager stateManager, string collection, Guid partitionId)
		{
			// Query the reliable collection.
			var query = context.Request.Query.Select(p => new KeyValuePair<string, string>(p.Key, p.Value));
			var result = await stateManager.QueryPartitionAsync(collection, query, partitionId, CancellationToken.None).ConfigureAwait(false);

			context.Response.ContentType = "application/json";
			context.Response.StatusCode = (int)HttpStatusCode.OK;

			// Write the response.
			var content = JsonConvert.SerializeObject(result);
			await context.Response.WriteAsync(content).ConfigureAwait(false);
		}

		private Task NotFound(HttpContext context)
		{
			context.Response.StatusCode = (int)HttpStatusCode.NotFound;
			context.Response.Headers.Add("X-ServiceFabric", "ResourceNotFound");

			return Task.CompletedTask;
		}

		private Task HandleCORS(HttpContext context)
		{
			context.Response.StatusCode = (int)HttpStatusCode.OK;

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

	public static class QueryableMiddlewareExtensions
	{
		public static IApplicationBuilder UseQueryable(this IApplicationBuilder app)
		{
			return app.UseMiddleware<QueryableMiddleware>();
		}
	}
}
