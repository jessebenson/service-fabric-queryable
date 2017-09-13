using Microsoft.ServiceFabric.Data;
using System;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Web.Http.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ODataExtensions
	{
		public static IAsyncEnumerable<T> ApplyTo<T>(this FilterQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			// Apply the filter on an empty enumerable to compile the filter expression.
			var emptySource = Enumerable.Empty<T>();
			var queryable = query.ApplyTo(emptySource.AsQueryable(), settings);

			// Validate the queryable was processed as we expect.
			var expression = queryable.Expression as MethodCallExpression;
			Contract.Assert(expression != null);
			Contract.Assert(expression.Arguments.Count == 2);

			// Extract the lambda expression, so we can apply it to an async enumerable.
			var unaryExpression = expression.Arguments[1] as UnaryExpression;
			Contract.Assert(unaryExpression != null);
			var predicateExpression = unaryExpression.Operand as LambdaExpression;
			Contract.Assert(predicateExpression != null);

			// Compile the lambda expression into a delegate.
			var predicate = predicateExpression.Compile();

			// Pass the filter delegate to the async where LINQ extension.
			MethodInfo whereMethod = ExpressionHelperMethods.AsyncWhereGeneric.MakeGenericMethod(typeof(T));
			return (IAsyncEnumerable<T>)whereMethod.Invoke(null, new object[] { source, predicate });
		}

		public static IAsyncEnumerable<T> ApplyTo<T>(this OrderByQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			throw new NotImplementedException();
		}

		public static IAsyncEnumerable<T> ApplyTo<T>(this SelectExpandQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			throw new NotImplementedException();
		}

		public static IAsyncEnumerable<T> ApplyTo<T>(this SkipQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			return source.SkipAsync(query.Value);
		}

		public static IAsyncEnumerable<T> ApplyTo<T>(this TopQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			return source.TakeAsync(query.Value);
		}
	}
}
