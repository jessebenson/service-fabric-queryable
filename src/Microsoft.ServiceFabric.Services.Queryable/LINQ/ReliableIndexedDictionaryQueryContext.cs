using Microsoft.Data.Edm;
using Microsoft.Data.OData.Query;
using Microsoft.Data.OData.Query.SemanticAst;
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
        internal static async Task<object> Execute<TKey, TValue, TResult>(Expression expression, IReliableStateManager stateManager, IReliableIndexedDictionary<TKey, TValue> dictionary)
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

            // Translate expression to Odata Expression
            SingleValueNode root = TranslateToOData(lambdaExpression.Body);
            IEnumerable<TKey> keys = await ReliableStateExtensions.TryFilterNode<TKey, TValue>(root, false, stateManager, dictionary.Name.AbsolutePath, new CancellationToken());

            IEnumerable<TValue> values;
            using (var tx = stateManager.CreateTransaction())
            {
                IEnumerable<KeyValuePair<TKey, TValue>> pairs = await dictionary.GetAllAsync(tx, keys, TimeSpan.FromSeconds(4), new CancellationToken());
                values = new KeyValueToValueEnumerable<TKey, TValue>(pairs);
                await tx.CommitAsync();
            }

            IQueryable<TValue> queryableValues = values.AsQueryable<TValue>();

            // Copy the expression tree that was passed in, changing only the first 
            // argument of the innermost MethodCallExpression.
            ExpressionTreeModifier<TKey, TValue, TResult> treeCopier = new ExpressionTreeModifier<TKey, TValue, TResult>(queryableValues);
            Expression newExpressionTree = treeCopier.Visit(expression);

            // This step creates an IQueryable that executes by replacing Queryable methods with Enumerable methods. 
            return queryableValues.Provider.CreateQuery(newExpressionTree);
        }

        private static SingleValueNode TranslateToOData(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("Expression argument is null");
            }

            if (expression is BinaryExpression asBinaryExpression)
            {
                if ((asBinaryExpression.Left is MemberExpression && asBinaryExpression.Right is ConstantExpression) ||
                     (asBinaryExpression.Left is ConstantExpression && asBinaryExpression.Right is MemberExpression))
                {
                    MemberExpression asMemberExpression = asBinaryExpression.Left is MemberExpression ? asBinaryExpression.Left as MemberExpression : asBinaryExpression.Right as MemberExpression;
                    ConstantExpression asConstantExpression = asBinaryExpression.Left is ConstantExpression ? asBinaryExpression.Left as ConstantExpression : asBinaryExpression.Right as ConstantExpression;

                    if (asConstantExpression == null)
                    {
                        throw new ArgumentNullException("Expression not well formed, constant expression null");
                    }
                    if (asMemberExpression == null)
                    {
                        throw new ArgumentNullException("Expression not well formed, member access expression null");
                    }

                    ConstantNode constantNode = (ConstantNode)TranslateToOData(asConstantExpression);
                    SingleValuePropertyAccessNode propertyAccessNode = new SingleValuePropertyAccessNode(new ConstantNode(null), new DynamicProperty(asMemberExpression.Member, constantNode.TypeReference));
                    return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), constantNode, propertyAccessNode);
                }

                return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), TranslateToOData(asBinaryExpression.Left), TranslateToOData(asBinaryExpression.Right));
            }
            else if (expression is UnaryExpression asUnaryExpression)
            {
                if (asUnaryExpression.NodeType == ExpressionType.Convert)
                {
                    return TranslateToOData(asUnaryExpression.Operand); // May be unsafe if convert needs to take place
                }

                return new UnaryOperatorNode(TranslateUnaryOperatorKind(asUnaryExpression.NodeType), TranslateToOData(asUnaryExpression.Operand));
            }
            else if (expression is ConstantExpression asConstantExpression)
            {
                if (asConstantExpression == null)
                {
                    throw new ArgumentNullException("Constant expression is null");
                }
                return new ConstantNode(asConstantExpression.Value);
            }
            else
            {
                throw new NotSupportedException("Cannot parse expression: " + expression.ToString());
            }
        }

        // Currently the only thing that is used by the expression parser is IEdmProperty.Name, which is only thing that will be emulated
        internal class DynamicProperty : IEdmProperty
        {
            public DynamicProperty(MemberInfo member, IEdmTypeReference reference)
            {
                Name = member.Name;
                PropertyKind = EdmPropertyKind.Structural;
                Type = reference;
                DeclaringType = new DynamicStructuredType();
            }

            public EdmPropertyKind PropertyKind { get; private set; }

            public IEdmTypeReference Type { get; private set; }

            public IEdmStructuredType DeclaringType { get; private set; }

            public string Name { get; private set; }
        }

        internal class DynamicStructuredType : IEdmStructuredType
        {
            public bool IsAbstract => false;

            public bool IsOpen => false;

            public IEdmStructuredType BaseType => this;

            public IEnumerable<IEdmProperty> DeclaredProperties => new List<IEdmProperty>();

            public EdmTypeKind TypeKind => EdmTypeKind.Complex;

            public IEdmProperty FindProperty(string name)
            {
                return null;
            }
        }


        private static UnaryOperatorKind TranslateUnaryOperatorKind(ExpressionType type)
        {
            if (type == ExpressionType.Not)
            {
                return UnaryOperatorKind.Not;
            }
            else
            {
                throw new NotSupportedException("Cannot parse expressions with type: " + type.ToString());
            }
        }

        private static BinaryOperatorKind translateBinaryOperatorKind(ExpressionType type)
        {
            if (type == ExpressionType.And || type == ExpressionType.AndAlso)
            {
                return BinaryOperatorKind.And;
            }
            if (type == ExpressionType.Equal)
            {
                return BinaryOperatorKind.Equal;
            }
            if(type ==ExpressionType.NotEqual)
            {
                return BinaryOperatorKind.NotEqual;
            }
            if (type == ExpressionType.GreaterThan)
            {
                return BinaryOperatorKind.GreaterThan;
            }
            if (type == ExpressionType.GreaterThanOrEqual)
            {
                return BinaryOperatorKind.GreaterThanOrEqual ;
            }
            if (type == ExpressionType.LessThan)
            {
                return BinaryOperatorKind.LessThan;
            }
            if (type == ExpressionType.LessThanOrEqual)
            {
                return BinaryOperatorKind.LessThanOrEqual;
            }
            if (type == ExpressionType.Or || type == ExpressionType.OrElse)
            {
                return BinaryOperatorKind.Or;
            }
            else
            {
                throw new NotSupportedException("Cannot parse expressions with type: " + type.ToString());
            }
        }

        private static bool IsQueryOverDataSource(Expression expression)
        {
            // If expression represents an unqueried IQueryable data source instance, 
            // expression is of type ConstantExpression, not MethodCallExpression. 
            return (expression is MethodCallExpression);
        }
    }
}