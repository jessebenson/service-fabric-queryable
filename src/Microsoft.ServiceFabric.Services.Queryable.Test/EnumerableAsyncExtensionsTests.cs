using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class EnumerableAsyncExtensionsTests
	{
		[TestMethod]
		public async Task CountAsync_Empty_ReturnsZero()
		{
			var enumerable = Enumerable.Empty<int>().AsAsyncEnumerable();
			Assert.AreEqual(0, await enumerable.CountAsync());
		}

		[TestMethod]
		public async Task CountAsync_Filter_Empty_ReturnsZero()
		{
			var enumerable = Enumerable.Empty<int>().AsAsyncEnumerable();
			Assert.AreEqual(0, await enumerable.CountAsync(i => i > 0));
		}

		[TestMethod]
		public async Task CountAsync_WithItems_ReturnsCount()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(4, await enumerable.CountAsync());
		}

		[TestMethod]
		public async Task CountAsync_Filter_WithItems_ReturnsCount()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(2, await enumerable.CountAsync(i => i >= 2));
		}

		[TestMethod]
		public async Task WhereAsync_Empty()
		{
			var enumerable = Enumerable.Empty<int>().AsAsyncEnumerable();
			Assert.AreEqual(0, await enumerable.WhereAsync(i => i > 0).CountAsync());
		}

		[TestMethod]
		public async Task WhereAsync_WithItems_ReturnsCount()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			var whereEnumerable = enumerable.WhereAsync(i => i < 2);

			Assert.AreEqual(2, await whereEnumerable.CountAsync());
			Assert.IsTrue(await whereEnumerable.ContainsAsync(0));
			Assert.IsTrue(await whereEnumerable.ContainsAsync(1));
		}

		[TestMethod]
		public async Task WhereAsync_FilterAll_ReturnsZero()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			var whereEnumerable = enumerable.WhereAsync(i => false);

			Assert.AreEqual(0, await whereEnumerable.CountAsync());
		}
	}
}
