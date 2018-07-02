using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static Microsoft.ServiceFabric.Services.Queryable.IEnumerableUtility;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
    [TestClass]
    public class IEnumerableUtilityTests
    {
        // Enumerables must be sorted in same way
        // For performance reasons method does not verify this
        IEnumerable<String> EmptyEnumerable = (new String[] { }).AsEnumerable();
        IEnumerable<String> FiveEnumerable = (new String[] { "A", "B", "C", "D", "E" }).AsEnumerable();
        IEnumerable<String> FiveEnumerable2 = (new String[] { "A", "B", "C", "Y", "Z" }).AsEnumerable();
        IEnumerable<String> ThreeEnumerable = (new String[] { "B", "C", "Z" }).AsEnumerable();

        [TestMethod]
        public void UnionTwoEmptyEnumerables()
        {
            IEnumerable<string> union = new UnionEnumerable<string>(EmptyEnumerable, EmptyEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual(EmptyEnumerable, union));
        }

        [TestMethod]
        public void IntersectTwoEmptyEnumerables()
        {
            IntersectEnumerable<string> intersection = new IntersectEnumerable<string>(EmptyEnumerable, EmptyEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual(EmptyEnumerable, intersection));
        }

        [TestMethod]
        public void IntersectSameSizeEnumerables()
        {
            IntersectEnumerable<string> intersection = new IntersectEnumerable<string>(FiveEnumerable, FiveEnumerable2);
            Assert.IsTrue(Enumerable.SequenceEqual((new[] { "A", "B", "C" }).AsEnumerable(), intersection));
        }

        [TestMethod]
        public void IntersectDifferentSizeEnumerables()
        {
            IntersectEnumerable<string> intersection = new IntersectEnumerable<string>(FiveEnumerable, ThreeEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual((new[] { "B", "C" }).AsEnumerable(), intersection));
        }

        [TestMethod]
        public void IntersectOneEmptyEnumerable()
        {
            IntersectEnumerable<string> intersection = new IntersectEnumerable<string>(FiveEnumerable, EmptyEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual(EmptyEnumerable, intersection));
        }

        [TestMethod]
        public void UnionSameSizeEnumerables()
        {
            UnionEnumerable<string> union = new UnionEnumerable<string>(FiveEnumerable, FiveEnumerable2);
            Assert.IsTrue(Enumerable.SequenceEqual((new[] { "A", "B", "C", "D", "E", "Y", "Z" }).AsEnumerable(), union));
        }

        [TestMethod]
        public void UnionDifferentSizeEnumerables()
        {
            UnionEnumerable<string> union = new UnionEnumerable<string>(FiveEnumerable, ThreeEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual((new[] { "A", "B", "C", "D", "E", "Z" }).AsEnumerable(), union));
        }

        [TestMethod]
        public void UnionOneEmptyEnumerable()
        {
            UnionEnumerable<string> union = new UnionEnumerable<string>(FiveEnumerable, EmptyEnumerable);
            Assert.IsTrue(Enumerable.SequenceEqual(FiveEnumerable, union));
        }
    }
}
