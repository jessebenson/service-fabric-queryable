using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Microsoft.ServiceFabric.Services.Queryable.LINQ
{
    internal class ExpressionTreeModifier<TKey, TValue, TResult> : ExpressionVisitor
        where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        private IQueryable<TValue> queryableValues;

        internal ExpressionTreeModifier(IQueryable<TValue> values)
        {
            this.queryableValues = values;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            // Replace the constant QueryableTerraServerData arg with the queryable Place collection. 
            if (c.Type == typeof(QueryableReliableIndexedDictionary<TKey, TValue, TResult>))
                return Expression.Constant(this.queryableValues);
            else
                return c;
        }
    }
}
