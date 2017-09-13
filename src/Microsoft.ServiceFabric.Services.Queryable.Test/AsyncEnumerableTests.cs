using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class AsyncEnumerableTests
	{
		[TestMethod]
		public async Task CountAsync_Empty_ReturnsZero()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
			Assert.AreEqual(0, await enumerable.CountAsync());
		}

		[TestMethod]
		public async Task CountAsync_Filter_Empty_ReturnsZero()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
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
		public async Task SelectAsync_Empty()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
			Assert.AreEqual(0, await enumerable.SelectAsync(i => (double)i).CountAsync());
		}

		[TestMethod]
		public async Task SelectAsync_ToDouble()
		{
			var enumerable = new int[] { 0, 1, 2, 3 }.AsAsyncEnumerable();
			var expected = new double[] { 0, 2, 4, 6 }.AsAsyncEnumerable();
			var selected = enumerable.SelectAsync(i => (double)i * 2);
			Assert.IsTrue(await selected.SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_Empty_Empty()
		{
			var first = AsyncEnumerable.EmptyAsync<int>();
			var second = AsyncEnumerable.EmptyAsync<int>();
			Assert.IsTrue(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_Empty_NonEmpty()
		{
			var first = AsyncEnumerable.EmptyAsync<int>();
			var second = Enumerable.Range(0, 2).AsAsyncEnumerable();
			Assert.IsFalse(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_NonEmpty_Empty()
		{
			var first = Enumerable.Range(0, 2).AsAsyncEnumerable();
			var second = AsyncEnumerable.EmptyAsync<int>();
			Assert.IsFalse(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_NoneEqual()
		{
			var first = Enumerable.Range(0, 2).AsAsyncEnumerable();
			var second = Enumerable.Range(10, 2).AsAsyncEnumerable();
			Assert.IsFalse(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_PartialEqual()
		{
			var first = Enumerable.Range(0, 2).AsAsyncEnumerable();
			var second = Enumerable.Range(0, 5).AsAsyncEnumerable();
			Assert.IsFalse(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SequenceEqualAsync_AllEqual()
		{
			var first = Enumerable.Range(0, 2).AsAsyncEnumerable();
			var second = Enumerable.Range(0, 2).AsAsyncEnumerable();
			Assert.IsTrue(await first.SequenceEqualAsync(second));
		}

		[TestMethod]
		public async Task SkipAsync_Empty()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
			Assert.AreEqual(0, await enumerable.SkipAsync(10).CountAsync());
		}

		[TestMethod]
		public async Task SkipAsync_SkipAll_ReturnsZero()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(0, await enumerable.SkipAsync(4).CountAsync());
		}

		[TestMethod]
		public async Task SkipAsync_SkipSome_ReturnsCount()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			var expected = Enumerable.Range(2, 2).AsAsyncEnumerable();
			Assert.AreEqual(2, await enumerable.SkipAsync(2).CountAsync());
			Assert.IsTrue(await enumerable.SkipAsync(2).SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task SkipAsync_SkipNone_ReturnsAll()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(4, await enumerable.SkipAsync(0).CountAsync());
			Assert.IsTrue(await enumerable.SkipAsync(0).SequenceEqualAsync(enumerable));
		}

		[TestMethod]
		public async Task TakeAsync_Empty()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
			Assert.AreEqual(0, await enumerable.TakeAsync(10).CountAsync());
		}

		[TestMethod]
		public async Task TakeAsync_TakeNone_ReturnsZero()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(0, await enumerable.TakeAsync(0).CountAsync());
		}

		[TestMethod]
		public async Task TakeAsync_TakeSome_ReturnsCount()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			var expected = Enumerable.Range(0, 2).AsAsyncEnumerable();
			Assert.AreEqual(2, await enumerable.TakeAsync(2).CountAsync());
			Assert.IsTrue(await enumerable.TakeAsync(2).SequenceEqualAsync(expected));
		}

		[TestMethod]
		public async Task TakeAsync_TakeAll_ReturnsAll()
		{
			var enumerable = Enumerable.Range(0, 4).AsAsyncEnumerable();
			Assert.AreEqual(4, await enumerable.TakeAsync(4).CountAsync());
			Assert.IsTrue(await enumerable.TakeAsync(4).SequenceEqualAsync(enumerable));
		}

		[TestMethod]
		public async Task WhereAsync_Empty()
		{
			var enumerable = AsyncEnumerable.EmptyAsync<int>();
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
