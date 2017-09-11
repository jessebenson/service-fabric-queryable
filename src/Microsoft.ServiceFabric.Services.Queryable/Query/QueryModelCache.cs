using System;
using System.Collections.Concurrent;
using System.Web.OData;
using System.Web.OData.Builder;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal sealed class QueryModelCache
	{
		private static readonly ConcurrentDictionary<Type, ODataQueryContext> ContextCache =
			new ConcurrentDictionary<Type, ODataQueryContext>();

		public ODataQueryContext GetQueryContext(Type type)
		{
			return ContextCache.GetOrAdd(type, CreateQueryContext);
		}

		private static ODataQueryContext CreateQueryContext(Type type)
		{
			var builder = new ODataConventionModelBuilder();
			builder.AddEntityType(type);
			var model = builder.GetEdmModel();
			return new ODataQueryContext(model, type, null);
		}
	}
}