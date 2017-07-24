using Microsoft.Data.OData;
using System;
using System.Collections.Generic;
using System.IO;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal class InMemoryMessage : IODataRequestMessage, IODataResponseMessage, IDisposable
	{
		private readonly Dictionary<string, string> _headers = new Dictionary<string, string>();

		public IEnumerable<KeyValuePair<string, string>> Headers => _headers;

		public int StatusCode { get; set; }

		public Uri Url { get; set; }

		public string Method { get; set; }

		public Stream Stream { get; set; }

		public IServiceProvider Container { get; set; }

		public string GetHeader(string headerName)
		{
			string headerValue;
			return _headers.TryGetValue(headerName, out headerValue) ? headerValue : null;
		}

		public void SetHeader(string headerName, string headerValue)
		{
			_headers[headerName] = headerValue;
		}

		public Stream GetStream()
		{
			return this.Stream;
		}

		public Action DisposeAction { get; set; }

		void IDisposable.Dispose()
		{
			DisposeAction?.Invoke();
		}
	}
}