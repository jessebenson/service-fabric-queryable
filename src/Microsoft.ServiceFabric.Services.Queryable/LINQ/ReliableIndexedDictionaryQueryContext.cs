using Microsoft.Data.OData.Query;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using static Microsoft.ServiceFabric.Services.Queryable.IEnumerableUtility;

namespace Microsoft.ServiceFabric.Services.Queryable.LINQ
{
    internal class ReliableIndexedDictionaryQueryContext
    {
        // Executes the expression tree that is passed to it. 
        // Should return 
        internal static object Execute<TKey, TValue, TResult>(Expression expression, IReliableStateManager stateManager, IReliableIndexedDictionary<TKey, TValue> dictionary)
                        where TKey : IComparable<TKey>, IEquatable<TKey>
        {
            // The expression must represent a query over the data source. 
            if (!IsQueryOverDataSource(expression))
                throw new InvalidProgramException("No query over the data source was specified.");

            // Find the call to Where() and get the lambda expression predicate.
            InnermostWhereFinder whereFinder = new InnermostWhereFinder();
            MethodCallExpression whereExpression = whereFinder.GetInnermostWhere(expression);
            LambdaExpression lambdaExpression = (LambdaExpression)((UnaryExpression)(whereExpression.Arguments[1])).Operand;

            // Send the lambda expression through the partial evaluator.
            lambdaExpression = (LambdaExpression)Evaluator.PartialEval(lambdaExpression);

            WhereExtractor<TResult> extractor = new WhereExtractor<TResult>(expression);
            BinaryOperatorKind operatorKind = extractor.OperatorKind;
            object constant = extractor.Constant;
            string propertyName = extractor.PropertyName;

            MethodInfo filterHelper = typeof(ReliableStateExtensions).GetMethod("FilterHelper", BindingFlags.Static | BindingFlags.Public);
            filterHelper = filterHelper.MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue), constant.GetType() });
            Task<IEnumerable<TKey>> keysTask = (Task<IEnumerable<TKey>>)filterHelper.Invoke(null, new object[] { dictionary, constant, operatorKind, false, new CancellationToken(), stateManager, propertyName});
            IEnumerable<TKey> keys = keysTask.Result;

            IEnumerable<TValue> values;
            using (var tx = stateManager.CreateTransaction())
            {
                IEnumerable<KeyValuePair<TKey, TValue>> pairs = dictionary.GetAllAsync(tx, keys, TimeSpan.FromSeconds(4), new CancellationToken()).Result;
                values = new KeyValueToValueEnumerable<TKey, TValue>(pairs);
                tx.CommitAsync();
            }

            IQueryable<TValue> queryableValues = values.AsQueryable<TValue>();

            // Copy the expression tree that was passed in, changing only the first 
            // argument of the innermost MethodCallExpression.
            ExpressionTreeModifier<TKey, TValue, TResult> treeCopier = new ExpressionTreeModifier<TKey, TValue, TResult>(queryableValues);
            Expression newExpressionTree = treeCopier.Visit(expression);

            // This step creates an IQueryable that executes by replacing Queryable methods with Enumerable methods. 
            return queryableValues.Provider.CreateQuery(newExpressionTree);
        }

        private static bool IsQueryOverDataSource(Expression expression)
        {
            // If expression represents an unqueried IQueryable data source instance, 
            // expression is of type ConstantExpression, not MethodCallExpression. 
            return (expression is MethodCallExpression);
        }
    }
}