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
			var enumerable = new AsyncEnumerable<int>(Enumerable.Empty<int>());
			Assert.AreEqual(0, await enumerable.CountAsync());
		}

		[TestMethod]
		public async Task CountAsync_Filter_Empty_ReturnsZero()
		{
			var enumerable = new AsyncEnumerable<int>(Enumerable.Empty<int>());
			Assert.AreEqual(0, await enumerable.CountAsync(i => i > 0));
		}

		[TestMethod]
		public async Task CountAsync_WithItems_ReturnsCount()
		{
			var enumerable = new AsyncEnumerable<int>(Enumerable.Range(0, 4));
			Assert.AreEqual(4, await enumerable.CountAsync());
		}

		[TestMethod]
		public async Task CountAsync_Filter_WithItems_ReturnsCount()
		{
			var enumerable = new AsyncEnumerable<int>(Enumerable.Range(0, 4));
			Assert.AreEqual(2, await enumerable.CountAsync(i => i >= 2));
		}
	}
}
