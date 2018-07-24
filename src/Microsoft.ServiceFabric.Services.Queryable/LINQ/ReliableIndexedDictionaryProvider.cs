using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using System.Reflection;
using Microsoft.ServiceFabric.Data;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable.LINQ
{
    public class ReliableIndexedDictionaryProvider<TKey, TValue> : IQueryProvider
        where TKey : IComparable<TKey>, IEquatable<TKey>
    {

        public IReliableIndexedDictionary<TKey, TValue> Dictionary { get; private set; }
        public IReliableStateManager StateManager { get; private set; }

        public ReliableIndexedDictionaryProvider (IReliableIndexedDictionary<TKey, TValue> indexedDictionary, IReliableStateManager stateManager)
        {
            Dictionary = indexedDictionary ?? throw new ArgumentNullException("Dictionary can not be null");
            StateManager = stateManager ?? throw new ArgumentNullException("StateManager can not be null");
        }

        public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = expression.Type.GetElementType();
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(QueryableReliableIndexedDictionary<,,>).MakeGenericType(new Type[] { typeof(TKey), typeof(TValue), elementType}), new object[] { this, expression });
            }
            catch (System.Reflection.TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public object Execute(Expression expression)
        {
            //Type elementType = expression.Type.GetElementType();
            //MethodInfo execute = typeof(ReliableIndexedDictionaryQueryContext).GetMethod("Execute", BindingFlags.Static).MakeGenericMethod(new Type[] { elementType });
            //return execute.Invoke(null, new object[] { expression });
            return ReliableIndexedDictionaryQueryContext.Execute<TKey, TValue>(expression, StateManager, Dictionary, false);
        }

        // Queryable's "single value" standard query operators call this method.
        // It is also called from QueryableTerraServerData.GetEnumerator(). 
        public TResult Execute<TResult>(Expression expression)
        {
            bool IsEnumerable = (typeof(TResult).Name == "IEnumerable`1");
            Type innerType = typeof(TResult).GenericTypeArguments[0];

            //MethodInfo execute = typeof(ReliableIndexedDictionaryQueryContext).GetMethod("Execute", BindingFlags.Static | BindingFlags.NonPublic);
            //execute = execute.MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue), innerType });
            //Task<object> result = ((Task<object>)execute.Invoke(null, new object[] { expression, StateManager, Dictionary, IsEnumerable }));
            return (TResult)ReliableIndexedDictionaryQueryContext.Execute<TKey, TValue>(expression, StateManager, Dictionary, IsEnumerable);
            //return (TResult)result.Result;
        }


        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new QueryableReliableIndexedDictionary<TKey, TValue, TElement>(this, expression);
        }
    }
}
