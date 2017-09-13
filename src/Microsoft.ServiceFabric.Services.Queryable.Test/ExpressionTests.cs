using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class ExpressionTests
	{
		[TestMethod]
		public async Task ExpressionTest()
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
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = filter.ApplyTo(users.AsAsyncEnumerable(), settings);

			Assert.AreEqual(5, await results.CountAsync());
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
