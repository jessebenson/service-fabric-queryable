using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
	[TestClass]
	public class ReflectionExtensionsTests
	{
		[TestMethod]
		public void ImplementsGenericType_NonInterface_Throws()
		{
			Assert.ThrowsException<ArgumentException>(() => 17.ImplementsGenericType(typeof(int)));
			Assert.ThrowsException<ArgumentException>(() => new object().ImplementsGenericType(typeof(object)));
			Assert.ThrowsException<ArgumentException>(() => new object().ImplementsGenericType(typeof(TestClass)));
			Assert.ThrowsException<ArgumentException>(() => new object().ImplementsGenericType(typeof(TestClassImplementsGenericInterface<>)));
			Assert.ThrowsException<ArgumentException>(() => new object().ImplementsGenericType(typeof(TestClassImplementsGenericInterface<object>)));
		}

		[TestMethod]
		public void ImplementsGenericType_NonGenericInterface_Throws()
		{
			Assert.ThrowsException<ArgumentException>(() => new TestClass().ImplementsGenericType(typeof(ITestInterface)));
			Assert.ThrowsException<ArgumentException>(() => new TestClassImplementsInterface().ImplementsGenericType(typeof(ITestInterface)));
			Assert.ThrowsException<ArgumentException>(() => new TestClass().ImplementsGenericType(typeof(ITestGenericInterface<object>)));
		}

		[TestMethod]
		public void ImplementsGenericType_ClassNotImplementGenericInterface_ReturnsFalse()
		{
			Assert.IsFalse(new TestClass().ImplementsGenericType(typeof(ITestGenericInterface<>)));
			Assert.IsFalse(new TestClassImplementsInterface().ImplementsGenericType(typeof(ITestGenericInterface<>)));
		}

		[TestMethod]
		public void ImplementsGenericType_ClassImplementsGenericInterface_ReturnsTrue()
		{
			Assert.IsTrue(new TestClassImplementsGenericInterface<int>().ImplementsGenericType(typeof(ITestGenericInterface<>)));
			Assert.IsTrue(new TestClassImplementsGenericInterface<string>().ImplementsGenericType(typeof(ITestGenericInterface<>)));
		}
	}
}
