using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Microsoft.ServiceFabric.Services.Queryable.LINQ
{
    // Working from:
    // https://msdn.microsoft.com/en-us/library/bb546158.aspx
    public class QueryableReliableIndexedDictionary<TKey, TValue, TResult> : IOrderedQueryable<TResult>
        where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        public QueryableReliableIndexedDictionary(IReliableIndexedDictionary<TKey, TValue> dictionary, IReliableStateManager stateManager)
        {
            Provider = new ReliableIndexedDictionaryProvider<TKey, TValue>(dictionary, stateManager);
            Expression = Expression.Constant(this);
        }

        public QueryableReliableIndexedDictionary(ReliableIndexedDictionaryProvider<TKey, TValue> provider, Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("Expression can not be null");
            }

            if (!typeof(IQueryable<TResult>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("Expression and Queryable types do not match");
            }

            Provider = provider ?? throw new ArgumentNullException("Provider can not be null");
            Expression = expression;

        }

        public Type ElementType => typeof(TResult);

        public Expression Expression { get; private set; }

        public IQueryProvider Provider { get; private set; }

        public IEnumerator<TResult> GetEnumerator()
        {
            var result = (Provider.Execute<IEnumerable<TResult>>(Expression));
            return result.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (Provider.Execute<System.Collections.IEnumerable>(Expression)).GetEnumerator();
        }
    }
}
