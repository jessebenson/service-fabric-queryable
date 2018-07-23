﻿using Microsoft.ServiceFabric.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
    class IEnumerableUtility
    {

        public sealed class KeyValueToValueEnumerable<TKey, TValue> : IEnumerable<TValue>
        {
            private IEnumerable<KeyValuePair<TKey, TValue>> enum1;

            public KeyValueToValueEnumerable(IEnumerable<KeyValuePair<TKey, TValue>> enum1)
            {
                this.enum1 = enum1;
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                return new KeyValueToValueEnumerator<TKey, TValue>(enum1.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private sealed class KeyValueToValueEnumerator<TKey, TValue> : IEnumerator<TValue>
        {
            private IEnumerator<KeyValuePair<TKey, TValue>> enum1;
            public TValue Current => enum1.Current.Value;

            object IEnumerator.Current => Current;

            public KeyValueToValueEnumerator(IEnumerator<KeyValuePair<TKey, TValue>> enum1)
            {
                this.enum1 = enum1;
            }

            public void Dispose()
            {
                enum1.Dispose();
            }

            public bool MoveNext()
            {
                return enum1.MoveNext();
            }

            public void Reset()
            {
                enum1.Reset();
            }
        }

        public sealed class KeyValueToValueAsyncEnumerable<TKey, TValue> : IAsyncEnumerable<TValue>
        {
            private IAsyncEnumerable<KeyValuePair<TKey, TValue>> enum1;

            public KeyValueToValueAsyncEnumerable(IAsyncEnumerable<KeyValuePair<TKey, TValue>> enum1)
            {
                this.enum1 = enum1;
            }

            public IAsyncEnumerator<TValue> GetAsyncEnumerator()
            {
                return new KeyValueToValueAsyncEnumerator<TKey, TValue>(enum1.GetAsyncEnumerator());
            }
        }

        private sealed class KeyValueToValueAsyncEnumerator<TKey, TValue> : IAsyncEnumerator<TValue>
        {
            private IAsyncEnumerator<KeyValuePair<TKey, TValue>> enum1;
            public TValue Current => enum1.Current.Value;

            public KeyValueToValueAsyncEnumerator(IAsyncEnumerator<KeyValuePair<TKey, TValue>> enum1)
            {
                this.enum1 = enum1;
            }

            public void Dispose()
            {
                enum1.Dispose();
            }

            public async Task<bool> MoveNextAsync(CancellationToken token)
            {
                return await enum1.MoveNextAsync(token);
            }

            public void Reset()
            {
                enum1.Reset();
            }
        }

        // IAsyncEnumerables must be in same sorted order!
        // Assumes no repeats
        public sealed class IntersectEnumerable<T> : IEnumerable<T>
            where T : IComparable<T>, IEquatable<T>
        {
            private IEnumerable<T> enum1, enum2;

            public IntersectEnumerable(IEnumerable<T> enum1, IEnumerable<T> enum2) 
            {
                this.enum1 = enum1;
                this.enum2 = enum2;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new IntersectEnumerator<T>(enum1.GetEnumerator(), enum2.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private sealed class IntersectEnumerator<T> : IEnumerator<T>
             where T : IComparable<T>, IEquatable<T>
        {
            private IEnumerator<T> enum1, enum2;
            private T current;
            public T Current => current;

            object IEnumerator.Current => Current;

            public IntersectEnumerator(IEnumerator<T> enum1, IEnumerator<T> enum2)
            {
                this.enum1 = enum1;
                this.enum2 = enum2;
            }

            public void Dispose()
            {
                enum1.Dispose();
                enum2.Dispose();
            }

            public bool MoveNext()
            {
                bool hasNext1 = enum1.MoveNext();
                bool hasNext2 = enum2.MoveNext();

                while(hasNext1 && hasNext2)
                {
                   if (enum1.Current.Equals(enum2.Current))
                    {
                        current = enum1.Current;
                        return true;
                    }
                   else if (enum1.Current.CompareTo(enum2.Current) < 0)
                    {
                        hasNext1 = enum1.MoveNext();
                    }
                   else if ((enum1.Current.CompareTo(enum2.Current) > 0))
                    {
                        hasNext2 = enum2.MoveNext();
                    }
                   else
                    {
                        throw new NotSupportedException("Impossible state for IAsyncEnumerators");
                    }
                }

                return false;
            }

            public void Reset()
            {
                enum1.Reset();
                enum2.Reset();
            }
        }

        // IAsyncEnumerables must be in same sorted order!
        // Assumes no repeats
        public sealed class UnionEnumerable<T> : IEnumerable<T>
            where T : IComparable<T>, IEquatable<T>
        {
            private IEnumerable<T> enum1, enum2;

            public UnionEnumerable(IEnumerable<T> enum1, IEnumerable<T> enum2)
            {
                this.enum1 = enum1;
                this.enum2 = enum2;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new UnionEnumerator<T>(enum1.GetEnumerator(), enum2.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private sealed class UnionEnumerator<T> : IEnumerator<T>
             where T : IComparable<T>, IEquatable<T>
        {
            private IEnumerator<T> enum1, enum2;
            private T current;
            public T Current => current;

            object IEnumerator.Current => Current;

            public UnionEnumerator(IEnumerator<T> enum1, IEnumerator<T> enum2)
            {
                this.enum1 = enum1;
                this.enum2 = enum2;
            }

            public void Dispose()
            {
                enum1.Dispose();
                enum2.Dispose();
            }

            private bool firstMoveNextAsync = true;
            private bool hasNext1;
            private bool hasNext2;

            public bool MoveNext()
            {
                if (firstMoveNextAsync)
                {
                    firstMoveNextAsync = false;
                    hasNext1 = enum1.MoveNext();
                    hasNext2 = enum2.MoveNext();
                }

                if (hasNext1 && hasNext2)
                {
                    if (enum1.Current.Equals(enum2.Current))
                    {
                        current = enum1.Current;
                        hasNext1 = enum1.MoveNext();
                        hasNext2 = enum2.MoveNext();
                        return true;
                    }
                    else if (enum1.Current.CompareTo(enum2.Current) < 0)
                    {
                        current = enum1.Current;
                        hasNext1 = enum1.MoveNext();
                        return true;
                    }
                    else if (enum1.Current.CompareTo(enum2.Current) > 0)
                    {
                        current = enum2.Current;
                        hasNext2 = enum2.MoveNext();
                        return true;
                    }
                    else
                    {
                        throw new NotSupportedException("Impossible state for IAsyncEnumerators");
                    }
                }
                else if (hasNext1)
                {
                    current = enum1.Current;
                    hasNext1 = enum1.MoveNext();
                    return true;
                }
                else if (hasNext2)
                {
                    current = enum2.Current;
                    hasNext2 = enum2.MoveNext();
                    return true;
                }

                return false;
            }

            public void Reset()
            {
                firstMoveNextAsync = true;
                enum1.Reset();
                enum2.Reset();
            }
        }
    }
}
