using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ODataExtensions
	{
		public static IAsyncEnumerable<T> ApplyTo<T>(this FilterQueryOption filter, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			// Apply the filter on an empty enumerable to compile the filter expression.
			var emptySource = Enumerable.Empty<T>();
			var queryable = filter.ApplyTo(emptySource.AsQueryable(), settings);
			var expression = queryable.Expression as MethodCallExpression;

			Contract.Assert(expression != null);

			// Extract the filter lambda, so we can apply it to an async enumerable.
			var predicateExpression = (expression.Arguments[1] as UnaryExpression).Operand as LambdaExpression;
			var predicate = predicateExpression.Compile();

			// Pass the filter lambda to the async where LINQ extension.
			MethodInfo whereMethod = ExpressionHelperMethods.AsyncWhereGeneric.MakeGenericMethod(typeof(T));
			return (IAsyncEnumerable<T>)whereMethod.Invoke(null, new object[] { source, predicate });
		}
	}
}
