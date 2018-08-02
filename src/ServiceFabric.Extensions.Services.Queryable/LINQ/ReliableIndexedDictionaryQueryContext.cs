using Microsoft.Data.Edm;
using Microsoft.Data.OData.Query;
using Microsoft.Data.OData.Query.SemanticAst;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using ServiceFabric.Extensions.Data.Indexing.Persistent;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using static ServiceFabric.Extensions.Services.Queryable.IEnumerableUtility;

namespace ServiceFabric.Extensions.Services.Queryable.LINQ
{
    internal class ReliableIndexedDictionaryQueryContext
    {
        // Executes the expression tree that is passed to it. 
        // Should return 
        internal static async Task<object> Execute<TKey, TValue>(Expression expression, IReliableStateManager stateManager, IReliableIndexedDictionary<TKey, TValue> dictionary, bool IsEnumerable)
                        where TKey : IComparable<TKey>, IEquatable<TKey>
        {
            //The expression must represent a query over the data source. 
            if (!IsQueryOverDataSource(expression))
                throw new InvalidProgramException("No query over the data source was specified.");

            // Find the call to Where() and get the lambda expression predicate.
            InnermostWhereFinder whereFinder = new InnermostWhereFinder();
            MethodCallExpression whereExpression = whereFinder.GetInnermostWhere(expression);
            IQueryable<TValue> queryableValues;

            if (whereExpression != null)
            {
                LambdaExpression lambdaExpression = (LambdaExpression)((UnaryExpression)(whereExpression.Arguments[1])).Operand;

                // Send the lambda expression through the partial evaluator.
                lambdaExpression = (LambdaExpression)Evaluator.PartialEval(lambdaExpression);

                // Translate expression to Odata Expression
                SingleValueNode root = TranslateToOData(lambdaExpression.Body);
                IEnumerable<TKey> keys = ReliableStateExtensions.TryFilterNode<TKey, TValue>(root, false, stateManager, dictionary.Name.AbsolutePath, new CancellationToken()).Result;

                IEnumerable<TValue> values;
                using (var tx = stateManager.CreateTransaction())
                {
                    IEnumerable<KeyValuePair<TKey, TValue>> pairs = await dictionary.GetAllAsync(tx, keys, TimeSpan.FromSeconds(4), new CancellationToken()).AsEnumerable();
                    values = new KeyValueToValueEnumerable<TKey, TValue>(pairs);
                    await tx.CommitAsync();
                }

                queryableValues = values.AsQueryable<TValue>();

            }
            else
            {
                IAsyncEnumerable<TValue> values;
                using (var tx = stateManager.CreateTransaction())
                {
                    IAsyncEnumerable<KeyValuePair<TKey, TValue>> pairs = dictionary.CreateEnumerableAsync(tx, EnumerationMode.Ordered).Result;
                    values = new KeyValueToValueAsyncEnumerable<TKey, TValue>(pairs);
                    await tx.CommitAsync();
                }

                queryableValues = (values.AsEnumerable().Result).AsQueryable<TValue>();
            }




            // Copy the expression tree that was passed in, changing only the first 
            // argument of the innermost MethodCallExpression.
            ExpressionTreeModifier<TKey, TValue> treeCopier = new ExpressionTreeModifier<TKey, TValue>(queryableValues);
            Expression newExpressionTree = treeCopier.Visit(expression);

            // This step creates an IQueryable that executes by replacing Queryable methods with Enumerable methods. 
            if (IsEnumerable)
                return queryableValues.Provider.CreateQuery(newExpressionTree);
            else
                return queryableValues.Provider.Execute(newExpressionTree);

            // This step creates an IQueryable that executes by replacing Queryable methods with Enumerable methods. 

        }

        private static SingleValueNode TranslateToOData(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("Expression argument is null");
            }

            if (expression is BinaryExpression asBinaryExpression)
            {
                // Since C# does not allow native <=, >= type operations on types like strings, we would like to still be able to take advantage of expressions that look like them
                if ((asBinaryExpression.Left is MethodCallExpression && asBinaryExpression.Right is ConstantExpression) ||
                     (asBinaryExpression.Left is ConstantExpression && asBinaryExpression.Right is MethodCallExpression))
                {
                    MethodCallExpression asMethodExpression = asBinaryExpression.Left is MethodCallExpression ? asBinaryExpression.Left as MethodCallExpression : asBinaryExpression.Right as MethodCallExpression;
                    ConstantExpression asConstantExpression = asBinaryExpression.Left is ConstantExpression ? asBinaryExpression.Left as ConstantExpression : asBinaryExpression.Right as ConstantExpression;

                    if (asConstantExpression == null)
                    {
                        throw new ArgumentNullException("Expression not well formed, constant expression null");
                    }
                    if (asMethodExpression == null)
                    {
                        throw new ArgumentNullException("Expression not well formed, method call expression null");
                    }

                    if (asMethodExpression.Method.Name != "CompareTo")
                    {
                        throw new NotSupportedException("Currently only CompareTo method calls are supported for indexing");
                    }

                    if (!(asConstantExpression.Value is int) || (int)asConstantExpression.Value != 0)
                    {
                        throw new NotSupportedException("CompareTo queries must be in form property.CompareTo(constant) 'operator' 0");
                    }

                    // We maintain the order of the arguments to maintain logical relationship
                    // eg 21 gt Age rather than Age gt 21
                    if (asMethodExpression.Object is ConstantExpression)
                    {
                        ConstantNode constantNode = (ConstantNode)TranslateToOData(asMethodExpression.Object);
                        SingleValuePropertyAccessNode propertyAccessNode = new SingleValuePropertyAccessNode(new ConstantNode(null), new DynamicProperty(((MemberExpression)asMethodExpression.Arguments[0]).Member, constantNode.TypeReference));
                        return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), constantNode, propertyAccessNode);
                    }
                    else
                    {
                        ConstantNode constantNode = (ConstantNode)TranslateToOData(asMethodExpression.Arguments[0]);
                        SingleValuePropertyAccessNode propertyAccessNode = new SingleValuePropertyAccessNode(new ConstantNode(null), new DynamicProperty(((MemberExpression)asMethodExpression.Object).Member, constantNode.TypeReference));
                        return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), propertyAccessNode, constantNode);
                    }
                }

                // AND, OR, GT, LT, GE, LE
                else if ((asBinaryExpression.Left is MemberExpression && asBinaryExpression.Right is ConstantExpression) ||
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
                    // This is some translation magic that makes the OData nodes operable but also contains some filler to satisfy system.
                    SingleValuePropertyAccessNode propertyAccessNode = new SingleValuePropertyAccessNode(new ConstantNode(null), new DynamicProperty(asMemberExpression.Member, constantNode.TypeReference));

                    // We maintain the order of the arguments to maintain logical relationship
                    // eg 21 gt Age rather than Age gt 21
                    if (asBinaryExpression.Left is ConstantExpression)
                    {
                        return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), constantNode, propertyAccessNode);
                    }
                    else
                    {
                        return new BinaryOperatorNode(translateBinaryOperatorKind(asBinaryExpression.NodeType), propertyAccessNode, constantNode);
                    }
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