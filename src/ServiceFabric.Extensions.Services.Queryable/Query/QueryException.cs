using System;
using System.Net;

namespace ServiceFabric.Extensions.Services.Queryable
{
	internal sealed class QueryException : Exception
	{
		public HttpStatusCode Status { get; }

		public QueryException(HttpStatusCode status, string message) : base(message)
		{
			Status = status;
		}
	}
}
