using Microsoft.ServiceFabric.Data;
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ExpressionHelperMethods
	{
		private static MethodInfo _whereExpression = GenericMethodOf(_ => AsyncEnumerable.WhereAsync(default(IAsyncEnumerable<int>), default(Func<int, bool>)));
		private static MethodInfo _selectExpression = GenericMethodOf(_ => AsyncEnumerable.SelectAsync(default(IAsyncEnumerable<int>), default(Func<int, int>)));

		public static MethodInfo AsyncWhereGeneric => _whereExpression;
		public static MethodInfo AsyncSelectGeneric => _selectExpression;

		private static MethodInfo GenericMethodOf<TReturn>(Expression<Func<object, TReturn>> expression)
		{
			return GenericMethodOf(expression as Expression);
		}

		private static MethodInfo GenericMethodOf(Expression expression)
		{
			LambdaExpression lambdaExpression = expression as LambdaExpression;

			Contract.Assert(expression.NodeType == ExpressionType.Lambda);
			Contract.Assert(lambdaExpression != null);
			Contract.Assert(lambdaExpression.Body.NodeType == ExpressionType.Call);

			return (lambdaExpression.Body as MethodCallExpression).Method.GetGenericMethodDefinition();
		}
	}
}
