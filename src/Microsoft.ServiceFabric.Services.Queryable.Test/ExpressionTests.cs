using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Reflection;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class ExpressionTests
	{
		private static MethodInfo _whereExpression = GenericMethodOf(_ => Enumerable.Where<int>(null, p => true));

		private static MethodInfo GenericMethodOf<TReturn>(Expression<Func<object, TReturn>> expression)
		{
			return GenericMethodOf(expression as Expression);
		}

		private static MethodInfo GenericMethodOf(Expression expression)
		{
			LambdaExpression lambdaExpression = expression as LambdaExpression;

			if (expression.NodeType != ExpressionType.Lambda)
				throw new ArgumentException();
			if (lambdaExpression == null)
				throw new ArgumentException();
			if (lambdaExpression.Body.NodeType != ExpressionType.Call)
				throw new ArgumentException();

			return (lambdaExpression.Body as MethodCallExpression).Method.GetGenericMethodDefinition();
		}

		[TestMethod]
		public void ExpressionTest()
		{
			var type = typeof(UserProfile);

			var users = Enumerable.Range(0, 10).Select(i => new UserProfile
			{
				Name = $"Name {i}",
				Age = 20 + i,
				Address = new Address
				{
					AddressLine1 = $"{i} Main St.",
					City = "Redmond",
					State = "WA",
					Zipcode = 98052,
				}
			});

			// OData context.
			var builder = new ODataConventionModelBuilder();
			builder.AddEntity(type);
			var model = builder.GetEdmModel();
			var context = new ODataQueryContext(model, type);

			// Filter.
			var filter = new FilterQueryOption("Age lt 25", context);

			// Pull out lambda expression.
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var result = filter.ApplyTo(new UserProfile[0].AsQueryable(), settings);

			var expression = result.Expression as MethodCallExpression;
			var lambdaExpression = (expression.Arguments[1] as UnaryExpression).Operand as LambdaExpression;
			var lambdaDelegate = lambdaExpression.Compile();

			MethodInfo whereMethod = _whereExpression.MakeGenericMethod(type);
			var enumerable = (IEnumerable<UserProfile>)whereMethod.Invoke(null, new object [] { users, lambdaDelegate });
			Assert.AreEqual(5, enumerable.Count());
		}
	}

	internal class UserProfile
	{
		public string Name { get; set; }
		public int Age { get; set; }
		public Address Address { get; set; }
	}

	internal class Address
	{
		public string AddressLine1 { get; set; }
		public string AddressLine2 { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public int Zipcode { get; set; }
	}
}
