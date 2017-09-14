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
			Type elementType = query.Context.ElementClrType;

			// Apply the operation on an empty enumerable to compile the filter expression.
			var emptySource = Array.CreateInstance(elementType, 0);
			var queryable = query.ApplyTo(emptySource.AsQueryable(), settings);

			// Validate the queryable was processed as we expect.
			var expression = queryable.Expression as MethodCallExpression;
			Contract.Assert(expression != null);
			Contract.Assert(expression.Arguments.Count == 2);

			// Extract the lambda expression, so we can apply it to an async enumerable.
			var unaryExpression = expression.Arguments[1] as UnaryExpression;
			Contract.Assert(unaryExpression != null);
			var lambdaExpression = unaryExpression.Operand as LambdaExpression;
			Contract.Assert(lambdaExpression != null);

			// Compile the lambda expression into a delegate.
			var predicate = lambdaExpression.Compile();

			// Pass the filter delegate to the async where LINQ extension.
			MethodInfo whereMethod = ExpressionHelperMethods.AsyncWhereGeneric.MakeGenericMethod(elementType);
			return (IAsyncEnumerable<T>)whereMethod.Invoke(null, new object[] { source, predicate });
		}

		public static IAsyncEnumerable<T> ApplyTo<T>(this OrderByQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			throw new NotImplementedException();
		}

		public static IAsyncEnumerable<object> ApplyTo<T>(this SelectExpandQueryOption query, IAsyncEnumerable<T> source, ODataQuerySettings settings)
		{
			Type elementType = query.Context.ElementClrType;

			// Apply the operation on an empty enumerable to compile the filter expression.
			var emptySource = Array.CreateInstance(elementType, 0);
			var queryable = query.ApplyTo(emptySource.AsQueryable(), settings);

			// Validate the queryable was processed as we expect.
			var expression = queryable.Expression as MethodCallExpression;
			Contract.Assert(expression != null);
			Contract.Assert(expression.Arguments.Count == 2);

			// Extract the lambda expression, so we can apply it to an async enumerable.
			var unaryExpression = expression.Arguments[1] as UnaryExpression;
			Contract.Assert(unaryExpression != null);
			var lambdaExpression = unaryExpression.Operand as LambdaExpression;
			Contract.Assert(lambdaExpression != null);

			// Compile the lambda expression into a delegate.
			var selector = lambdaExpression.Compile();

			// Pass the select delegate to the async where LINQ extension.
			MethodInfo selectMethod = ExpressionHelperMethods.AsyncSelectGeneric.MakeGenericMethod(elementType, lambdaExpression.ReturnType);
			return (IAsyncEnumerable<object>)selectMethod.Invoke(null, new object[] { source, selector });
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
