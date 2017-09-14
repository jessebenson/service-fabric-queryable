using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal class ODataQueryOptions
	{
		/// <summary>
		/// Maximum number of items the query is allowed to return.
		/// </summary>
		private const int MaxTop = 100;

		public FilterQueryOption Filter { get; set; }
		public OrderByQueryOption OrderBy { get; set; }
		public SelectExpandQueryOption Select { get; set; }
		public TopQueryOption Top { get; set; }

		public ODataQueryOptions(IEnumerable<KeyValuePair<string, string>> queryParameters, ODataQueryContext context, bool aggregate)
		{
			if (queryParameters == null)
				throw new ArgumentNullException(nameof(queryParameters));

			foreach (var queryParameter in queryParameters)
			{
				switch (queryParameter.Key)
				{
					case "$filter":
						if (!aggregate)
						{
							Filter = new FilterQueryOption(queryParameter.Value, context);
						}
						break;

					case "$orderby":
						OrderBy = new OrderByQueryOption(queryParameter.Value, context);
						break;

					case "$select":
						if (aggregate)
						{
							Select = new SelectExpandQueryOption(queryParameter.Value, string.Empty, context);
						}
						break;

					case "$top":
						Top = new TopQueryOption(queryParameter.Value, context);
						break;

					case "$format":
						break;

					default:
						throw new ArgumentException($"'{queryParameter.Key}' option is not supported");
				}
			}

			if (Top == null || Top.Value > MaxTop)
			{
				Top = new TopQueryOption($"{MaxTop}", context);
			}
		}

		public IQueryable ApplyTo(IQueryable queryable, ODataQuerySettings settings)
		{
			IQueryable result = queryable;
			if (Filter != null)
			{
				result = Filter.ApplyTo(result, settings);
			}
			if (OrderBy != null)
			{
				result = OrderBy.ApplyTo(result, settings);
			}
			if (Top != null)
			{
				result = Top.ApplyTo(result, settings);
			}
			if (Select != null)
			{
				result = Select.ApplyTo(result, settings);
			}

			return result;
		}

		public IAsyncEnumerable<object> ApplyTo(IAsyncEnumerable<object> enumerable, ODataQuerySettings settings)
		{
			IAsyncEnumerable<object> result = enumerable;
			if (Filter != null)
			{
				result = Filter.ApplyTo(result, settings);
			}
			if (OrderBy != null)
			{
				result = OrderBy.ApplyTo(result, settings);
			}
			if (Top != null)
			{
				result = Top.ApplyTo(result, settings);
			}
			if (Select != null)
			{
				result = Select.ApplyTo<object>(result, settings);
			}

			return result;
		}
	}
}