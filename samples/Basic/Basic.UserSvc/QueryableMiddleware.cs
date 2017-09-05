using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Queryable;
using System.Net;
using System.Threading.Tasks;

namespace Basic.UserSvc
{
	public sealed class QueryableMiddleware
	{
		private readonly RequestDelegate _next;

		public QueryableMiddleware(RequestDelegate next)
		{
			_next = next;
		}

		public Task Invoke(HttpContext context, IReliableStateManager stateManager)
		{
			var request = context.Request;
			if (request.Path.StartsWithSegments("/query"))
			{
				// Handle CORS.
				AddAccessControlHeaders(request, context.Response);
				if (request.Method == HttpMethods.Options)
					return HandleCORS(context);

				// Queryable handlers.
				if (request.Method == HttpMethods.Get && request.Path == "/query/$metadata")
					return GetMetadataAsync(context, stateManager);

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
