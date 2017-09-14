using Microsoft.ServiceFabric.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class ODataExtensionsTests
	{
		[TestMethod]
		public async Task FilterQueryOption_FilterNone()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new FilterQueryOption("Age ge 20", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			Assert.AreEqual(10, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(users));
		}

		[TestMethod]
		public async Task FilterQueryOption_FilterSome()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new FilterQueryOption("Age lt 25", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = users.WhereAsync(u => u.Age < 25);
			Assert.AreEqual(5, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task FilterQueryOption_FilterAll()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new FilterQueryOption("Age eq 0", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = AsyncEnumerable.EmptyAsync<UserProfile>();
			Assert.AreEqual(0, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task FilterQueryOption_Cast()
		{
			IAsyncEnumerable<object> users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new FilterQueryOption("Age ge 20", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			Assert.AreEqual(10, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(users));
		}

		[TestMethod]
		public void OrderByQueryOption_NotImplemented()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new OrderByQueryOption("Age", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			Assert.ThrowsException<NotImplementedException>(() => query.ApplyTo(users, settings));
		}

		[TestMethod]
		public async Task SelectExpandQueryOption_NameAge()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new SelectExpandQueryOption("Name,Age", null, context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo<UserProfile>(users, settings);

			Assert.AreEqual(10, await results.CountAsync());

			// Validate the properties got selected correctly.
			using (var u = users.GetAsyncEnumerator())
			using (var r = results.GetAsyncEnumerator())
			{
				var token = CancellationToken.None;
				while (await u.MoveNextAsync(token) && await r.MoveNextAsync(token))
				{
					var obj = JObject.FromObject(r.Current);
					Assert.AreEqual(u.Current.Age, obj.Property("Age").Value.Value<int>());
					Assert.AreEqual(u.Current.Name, obj.Property("Name").Value.Value<string>());
				}
			}
		}

		[TestMethod]
		public async Task SkipQueryOption_SkipNone()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new SkipQueryOption("0", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			Assert.AreEqual(10, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(users));
		}

		[TestMethod]
		public async Task SkipQueryOption_SkipSome()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new SkipQueryOption("5", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = users.SkipAsync(5);
			Assert.AreEqual(5, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task SkipQueryOption_SkipAll()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new SkipQueryOption("10", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = AsyncEnumerable.EmptyAsync<UserProfile>();
			Assert.AreEqual(0, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task TopQueryOption_TakeAll()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new TopQueryOption("10", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			Assert.AreEqual(10, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(users));
		}

		[TestMethod]
		public async Task TopQueryOption_TakeSome()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new TopQueryOption("5", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = users.TakeAsync(5);
			Assert.AreEqual(5, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task TopQueryOption_TakeNone()
		{
			var users = GetUsers(20, 30);
			var context = GetQueryContext();

			var query = new TopQueryOption("0", context);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var results = query.ApplyTo(users, settings);

			var expected = AsyncEnumerable.EmptyAsync<UserProfile>();
			Assert.AreEqual(0, await results.CountAsync());
			Assert.IsTrue(await results.SequenceEqualAsync(expected));
		}

		private static ODataQueryContext GetQueryContext()
		{
			var type = typeof(UserProfile);
			var builder = new ODataConventionModelBuilder();
			builder.AddEntity(type);
			var model = builder.GetEdmModel();
			return new ODataQueryContext(model, type);
		}

		private static IAsyncEnumerable<UserProfile> GetUsers(int start, int end)
		{
			var users = Enumerable.Range(start, end - start).Select(age => new UserProfile
			{
				Name = $"Name {age}",
				Age = age,
				Address = new Address
				{
					AddressLine1 = $"{age} Main St.",
					City = "Redmond",
					State = "WA",
					Zipcode = 98052,
				}
			}).ToArray();

			return users.AsAsyncEnumerable();
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
