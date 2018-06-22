using Microsoft.Data.OData;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.OData.Builder;
using System.Web.Http.OData.Query;
using Microsoft.Data.OData.Query.SemanticAst;
using Microsoft.Data.Edm;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using System.Linq.Expressions;
using System.Linq.Dynamic;
using Microsoft.ServiceFabric.Services.Queryable.Enumerable;
using Microsoft.Data.OData.Query;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class ReliableStateExtensions
	{
		private static readonly QueryModelCache QueryCache = new QueryModelCache();

		/// <summary>
		/// Get the OData metadata about the reliable collections from the reliable state manager using reflection.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <returns>The OData metadata for this state manager.</returns>
		public static async Task<string> GetMetadataAsync(this IReliableStateManager stateManager)
		{
			// Build the OData model from the queryable types in the reliable state manager.
			var builder = new ODataConventionModelBuilder();
			foreach (var queryable in await stateManager.GetQueryableTypes().ConfigureAwait(false))
			{
				var entity = builder.AddEntity(queryable.Value);
				builder.AddEntitySet(queryable.Key, entity);
			}
			var model = builder.GetEdmModel();

			// Write the OData metadata document.
			using (var stream = new MemoryStream())
			using (var message = new InMemoryMessage { Stream = stream })
			{
				var settings = new ODataMessageWriterSettings();
				var writer = new ODataMessageWriter((IODataResponseMessage)message, settings, model);
				writer.WriteMetadataDocument();
				return Encoding.UTF8.GetString(stream.ToArray());
			}
		}

		/// <summary>
		/// Query the reliable collection with the given name from the reliable state manager using the given query parameters.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="context">Stateful Service Context.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The json serialized results of the query.</returns>
		public static async Task<IEnumerable<JToken>> QueryAsync(this IReliableStateManager stateManager,
			StatefulServiceContext context, string collection, IEnumerable<KeyValuePair<string, string>> query, CancellationToken cancellationToken)
		{
			// Get the list of partitions (excluding the executing partition).
			var partitions = await StatefulServiceUtils.GetPartitionsAsync(context).ConfigureAwait(false);

			// Query all service partitions concurrently.
			var remoteQueries = partitions.Select(p => QueryPartitionAsync(p, context, collection, query, cancellationToken));
			var localQuery = stateManager.QueryPartitionAsync(collection, query, context.PartitionId, cancellationToken);
			var queries = remoteQueries.Concat(new[] { localQuery });

			// Aggregate all query results into a single list.
			var queryResults = await Task.WhenAll(queries).ConfigureAwait(false);
			var results = queryResults.SelectMany(r => r);

			// Run the aggregation query to get the final results (e.g. for top, orderby, project).
			var reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);
			var entityType = reliableState.GetEntityType();
			var objects = results.Select(r => r.ToObject(entityType));
			var queryResult = ApplyQuery(objects, entityType, query, aggregate: true);
			results = queryResult.Select(q => JObject.FromObject(q));

			// Return the filtered data as json.
			return results;
		}

		/// <summary>
		/// Query the reliable collection with the given name from the reliable state manager using the given query parameters.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The json serialized results of the query.</returns>
		public static async Task<IEnumerable<JToken>> QueryPartitionAsync(this IReliableStateManager stateManager,
			string collection, IEnumerable<KeyValuePair<string, string>> query, Guid partitionId, CancellationToken cancellationToken)
		{
			// Find the reliable state (boilerplate)
			IReliableState reliableState = await stateManager.GetQueryableState(collection).ConfigureAwait(false);
            var entityType = reliableState.GetEntityType(); // Type Information about the dictionary
            // Types of the objects in the dictionary, used by rest of types
            Type dictionaryKeyType = entityType.GenericTypeArguments[0];
            Type dictionaryValueType = entityType.GenericTypeArguments[1];

            // Generate an ODataQuery Object
            var context = QueryCache.GetQueryContext(entityType);
            ODataQueryOptions queryOptions = new ODataQueryOptions(query, context, aggregate: false);

            MethodInfo tryFilterQuery = typeof(ReliableStateExtensions).GetMethod("TryFilterQuery", BindingFlags.NonPublic | BindingFlags.Static);
            tryFilterQuery = tryFilterQuery.MakeGenericMethod(new Type[] { dictionaryKeyType, dictionaryValueType });
            Task filteredKVTask = (Task)tryFilterQuery.Invoke(null, new object[] { queryOptions, stateManager, collection, cancellationToken });
            await filteredKVTask;
            dynamic filteredKV = filteredKVTask.GetType().GetProperty("Result").GetValue(filteredKVTask);

            // It was not filterable, have to use AsyncEnumerable over the original dictionary
            if (filteredKV == null)
            {
                // If the query does not contain a valid $filter clause or there is no secondary index for that $filter, does an entire dictionary lookup
                using (var tx = stateManager.CreateTransaction())
                {
                    // Get the data from the reliable state
                    var results = await reliableState.GetAsyncEnumerable(tx, stateManager, partitionId, cancellationToken).ConfigureAwait(false);

                    // Filter the data
                    // var entityType = reliableState.GetEntityType();
                    results = ApplyQuery(results, entityType, query, aggregate: false);

                    // Convert to json
                    var json = await results.SelectAsync(r => JObject.FromObject(r)).AsEnumerable().ConfigureAwait(false);

                    await tx.CommitAsync().ConfigureAwait(false);

                    // Return the filtered data as json
                    return json;
                }
            }
            else
            {
                // Turns List to IEnumerable, thens turn to IAsyncEnumerable, then turns to IAsyncEnumerable<Entity> which is what Query infra wants
                MethodInfo asAsyncEnumerableMethod = typeof(AsyncEnumerable).GetMethod("AsAsyncEnumerable").MakeGenericMethod(typeof(KeyValuePair<,>).MakeGenericType(new Type[] { dictionaryKeyType, dictionaryValueType })); ;
                filteredKV = asAsyncEnumerableMethod.Invoke(null, new object[] { filteredKV });
                MethodInfo asEntityMethod = typeof(ReliableStateExtensions).GetMethod("AsEntity", BindingFlags.NonPublic | BindingFlags.Static);
                asEntityMethod = asEntityMethod.MakeGenericMethod(new Type[] { dictionaryKeyType, dictionaryValueType });
                filteredKV = asEntityMethod.Invoke(null, new object[] { filteredKV, partitionId, cancellationToken });

                // Aplies the query to the results
                dynamic queryResults = ApplyQuery(filteredKV, entityType, query, aggregate: false);

                // Creates a lambda that will turn each element in results to a json object
                ParameterExpression objectParameterExpression = Expression.Parameter(typeof(object), "r"); //dictvaluetype is true type of object
                Expression jobjectExpression = Expression.Call(typeof(JObject).GetMethod("FromObject", new Type[] { typeof(object) }), objectParameterExpression);
                LambdaExpression jsonLambda = Expression.Lambda(jobjectExpression, new ParameterExpression[] { objectParameterExpression });

                // Converts to json
                MethodInfo selectAsync = typeof(AsyncEnumerable).GetMethod("SelectAsync").MakeGenericMethod(new Type[] { typeof(object), typeof(JObject) });
                dynamic jsonAsyncEnumerable = selectAsync.Invoke(null, new object[] { queryResults, (Func<object, JObject>)jsonLambda.Compile() });

                // Converts json return to Enumerable of json
                MethodInfo asyncEnumerableAsEnumerableMethod = typeof(AsyncEnumerable).GetMethod("AsEnumerable").MakeGenericMethod(typeof(JObject));
                Task jsonTask = (Task)asyncEnumerableAsEnumerableMethod.Invoke(null, new object[] { jsonAsyncEnumerable, default(CancellationToken) });
                await jsonTask;
                IEnumerable<JObject> json = (IEnumerable<JObject>)jsonTask.GetType().GetProperty("Result").GetValue(jsonTask);

                // Return the filtered data as json
                return json;
            }
		}

        private static async Task<IReliableIndexedDictionary<TKey, TValue>> GetIndexedDictionaryByPropertyName<TKey, TValue, TFilter>(IReliableStateManager stateManager, string dictName, string propertyName)
                    where TKey : IComparable<TKey>, IEquatable<TKey>
                    where TFilter : IComparable<TFilter>, IEquatable<TFilter>
        {
            FilterableIndex<TKey, TValue, TFilter> filter = FilterableIndex<TKey, TValue, TFilter>.CreateQueryableInstance(propertyName);
            ConditionalValue<IReliableIndexedDictionary<TKey, TValue>> dictOption = await stateManager.TryGetIndexedAsync<TKey, TValue>(dictName, new[] { filter });
            if (dictOption.HasValue) {
                return dictOption.Value;
            }
            else
            {
                return null;
            }
        }

        private static async Task<IEnumerable<KeyValuePair<TKey, TValue>>> TryFilterQuery<TKey, TValue>(ODataQueryOptions options, IReliableStateManager stateManager, string dictName, CancellationToken cancellationToken)
            where TKey : IComparable<TKey>, IEquatable<TKey>
        {
            if (options.Filter == null)
            {
                return null;
            }
            SingleValueNode root = options.Filter.FilterClause.Expression;
            IEnumerable<TKey> filteredKeys = await TryFilterNode<TKey, TValue>(root, false, stateManager, dictName, cancellationToken);

            // Expression was not filterable
            if (filteredKeys == null)
            {
                return null;
            }
            else
            {
                ConditionalValue<IReliableIndexedDictionary<TKey, TValue>> dictOption = await stateManager.TryGetIndexedAsync<TKey, TValue>(dictName, new IIndexDefinition<TKey, TValue>[] { } );
                if (dictOption.HasValue)
                {
                    using (var tx = stateManager.CreateTransaction())
                    {
                        IEnumerable<KeyValuePair<TKey, TValue>> result = await dictOption.Value.GetAllAsync(tx, filteredKeys, TimeSpan.FromSeconds(4), cancellationToken);
                        await tx.CommitAsync();
                        return result;
                    }
                }

                return null; // Baseline Dictionary not found
            }
        }

        private static async Task<IEnumerable<TKey>> TryFilterNode<TKey, TValue>(SingleValueNode node, bool notIsApplied, IReliableStateManager stateManager, string dictName, CancellationToken cancellationToken)
            where TKey : IComparable<TKey>, IEquatable<TKey>
        {
            if (node is UnaryOperatorNode)
            {
                // If NOT, negate tree and return isFilterable
                UnaryOperatorNode asUONode = node as UnaryOperatorNode;
                if (asUONode.OperatorKind == Microsoft.Data.OData.Query.UnaryOperatorKind.Not)
                {
                    if (isFilterableNode2(asUONode.Operand, !notIsApplied))
                    {
                        return await TryFilterNode<TKey, TValue>(asUONode.Operand, !notIsApplied, stateManager, dictName, cancellationToken);
                    }

                    return null;
                }
                else
                {
                    throw new NotSupportedException("Does not support the Negate unary operator");
                }
            }
            else if (node is BinaryOperatorNode)
            {
                // Filterable(A) AND Filterable(B)      => Intersect(Filter(A), Filter(B))
                // !Filterable(A) AND Filterable(B)     => Filter(B)
                // Filterable(A) AND !Filterable(B)     => Filter(A)
                // !Filterable(A) AND !Filterable(B)    => NF
                BinaryOperatorNode asBONode = node as BinaryOperatorNode;
                if ((asBONode.OperatorKind == BinaryOperatorKind.And && !notIsApplied) ||
                    (asBONode.OperatorKind == BinaryOperatorKind.Or && notIsApplied))
                {
                    bool leftFilterable = isFilterableNode2(asBONode.Left, notIsApplied);
                    bool rightFilterable = isFilterableNode2(asBONode.Right, notIsApplied);

                    // Both are filterable: intersect
                    if (leftFilterable && rightFilterable)
                    {
                        return new IEnumerableUtility.IntersectEnumerable<TKey>(await TryFilterNode<TKey,TValue>(asBONode.Left, notIsApplied, stateManager, dictName, cancellationToken), 
                                                                                await TryFilterNode<TKey,TValue>(asBONode.Right, notIsApplied, stateManager, dictName, cancellationToken));
                    }
                    else if (leftFilterable)
                    {
                        return await TryFilterNode<TKey, TValue>(asBONode.Left, notIsApplied, stateManager, dictName, cancellationToken);
                    }
                    else if (rightFilterable)
                    {
                        return await TryFilterNode<TKey, TValue>(asBONode.Right, notIsApplied, stateManager, dictName, cancellationToken);
                    }
                    else
                    {
                        return null; // This should never be hit because if !Filterable(Left) && !Filterable(Right) => !Filterable(Me)
                    }

                }
                // Filterable(A) OR Filterable(B)      => Union(Filter(A), Filter(B))
                // !Filterable(A) OR Filterable(B)     => NF
                // Filterable(A) OR !Filterable(B)     => NF
                // !Filterable(A) OR !Filterable(B)    => NF
                else if ((asBONode.OperatorKind == BinaryOperatorKind.Or && !notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.And && notIsApplied))
                {
                    bool leftFilterable = isFilterableNode2(asBONode.Left, notIsApplied);
                    bool rightFilterable = isFilterableNode2(asBONode.Right, notIsApplied);

                    // Both are filterable: intersect
                    if (leftFilterable && rightFilterable)
                    {
                        return new IEnumerableUtility.UnionEnumerable<TKey>(await TryFilterNode<TKey,TValue>(asBONode.Left, notIsApplied, stateManager, dictName, cancellationToken), 
                                                                            await TryFilterNode<TKey,TValue>(asBONode.Right, notIsApplied, stateManager, dictName, cancellationToken));
                    }

                    return null;
                }
                // If Equals, >=, >, <, <=
                else if ((asBONode.OperatorKind == BinaryOperatorKind.Equal && !notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.NotEqual && notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.GreaterThan) || 
                         (asBONode.OperatorKind == BinaryOperatorKind.GreaterThanOrEqual) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.LessThan) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.LessThanOrEqual))
                {
                    // Resolve the arbitrary order of the request
                    SingleValuePropertyAccessNode valueNode = asBONode.Left is SingleValuePropertyAccessNode ? asBONode.Left as SingleValuePropertyAccessNode : asBONode.Right as SingleValuePropertyAccessNode;
                    ConstantNode constantNode = asBONode.Left is ConstantNode ? asBONode.Left as ConstantNode : asBONode.Right as ConstantNode;

                    string propertyName = valueNode.Property.Name;
                    Type propertyType = constantNode.Value.GetType(); //Possible reliance on type bad if name of property and provided type conflict?

                    MethodInfo getIndexedDictionaryByPropertyName = typeof(ReliableStateExtensions).GetMethod("GetIndexedDictionaryByPropertyName", BindingFlags.NonPublic | BindingFlags.Static);
                    getIndexedDictionaryByPropertyName = getIndexedDictionaryByPropertyName.MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue), propertyType });
                    Task indexedDictTask = (Task)getIndexedDictionaryByPropertyName.Invoke(null, new object[] { stateManager, dictName, propertyName });
                    await indexedDictTask;
                    var indexedDict = indexedDictTask.GetType().GetProperty("Result").GetValue(indexedDictTask);

                    if (indexedDict == null)
                    {
                        return null; // Filter does not exist or dictionary does not exist
                    }

                    MethodInfo filterHelper = typeof(ReliableStateExtensions).GetMethod("filterHelper", BindingFlags.NonPublic | BindingFlags.Static);
                    filterHelper = filterHelper.MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue), propertyType });
                    Task filterHelperTask = (Task)filterHelper.Invoke(null, new object[] { indexedDict, constantNode.Value, asBONode.OperatorKind, notIsApplied, cancellationToken, stateManager, propertyName });
                    await filterHelperTask;
                    return (IEnumerable<TKey>)filterHelperTask.GetType().GetProperty("Result").GetValue(filterHelperTask);
                }
                // We choose to mark NotEquals as unfilterable. Theoretically with indexes with low number of keys may be slightly faster than not-filtering
                // But generally is same order of magnitude as not using filters
                else if ((asBONode.OperatorKind == BinaryOperatorKind.Equal && notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.NotEqual && !notIsApplied))
                {
                    return null;
                }
                else
                {
                    throw new NotSupportedException("Does not support Add, Subtract, Modulo, Multiply, Divide operations.");
                }
            }
            else if (node is ConvertNode)
            {
                ConvertNode asCNode = node as ConvertNode;
                return await TryFilterNode<TKey, TValue>(asCNode.Source, notIsApplied, stateManager, dictName, cancellationToken);
            }
            else
            {
                throw new NotSupportedException("Only supports Binary and Unary operator nodes");
            }
        }

        // This is a seperate method because we now know PropertyType, so want to not use reflection in above method
        private static async Task<IEnumerable<TKey>> filterHelper<TKey, TValue, TFilter>(ReliableIndexedDictionary<TKey, TValue> dictionary, TFilter constant, BinaryOperatorKind strategy, bool notIsApplied, CancellationToken cancellationToken, IReliableStateManager stateManager, string propertyName)
            where TFilter : IComparable<TFilter>, IEquatable<TFilter>
            where TKey : IComparable<TKey>, IEquatable<TKey>
        {
            using (var tx = stateManager.CreateTransaction())
            {
                // Equals
                if ((strategy == BinaryOperatorKind.Equal && !notIsApplied) ||
                    (strategy == BinaryOperatorKind.NotEqual && notIsApplied))
                {
                    return await dictionary.FilterKeysOnlyAsync(tx, propertyName, constant, TimeSpan.FromSeconds(4), cancellationToken);
                }
                else if ((strategy == BinaryOperatorKind.GreaterThan && !notIsApplied) ||
                         (strategy == BinaryOperatorKind.LessThan && notIsApplied))
                {
                    return await dictionary.RangeFromFilterKeysOnlyAsync(tx, propertyName, constant, RangeFilterType.EXCLUSIVE, TimeSpan.FromSeconds(4), cancellationToken);
                }
                else if ((strategy == BinaryOperatorKind.GreaterThanOrEqual && !notIsApplied) ||
                         (strategy == BinaryOperatorKind.LessThanOrEqual && notIsApplied))
                {
                    return await dictionary.RangeFromFilterKeysOnlyAsync(tx, propertyName, constant, RangeFilterType.INCLUSIVE, TimeSpan.FromSeconds(4), cancellationToken);
                }
                else if ((strategy == BinaryOperatorKind.LessThan && !notIsApplied) ||
                         (strategy == BinaryOperatorKind.GreaterThan && notIsApplied))
                {
                    return await dictionary.RangeToFilterKeysOnlyAsync(tx, propertyName, constant, RangeFilterType.EXCLUSIVE, TimeSpan.FromSeconds(4), cancellationToken);
                }
                else if ((strategy == BinaryOperatorKind.LessThanOrEqual && !notIsApplied) ||
                         (strategy == BinaryOperatorKind.GreaterThanOrEqual && notIsApplied))
                {
                    return await dictionary.RangeToFilterKeysOnlyAsync(tx, propertyName, constant, RangeFilterType.INCLUSIVE, TimeSpan.FromSeconds(4), cancellationToken);
                }
                else
                {
                    // Bad State, should never hit
                    throw new NotSupportedException("Does not support Add, Subtract, Modulo, Multiply, Divide operations.");
                }
            }

        }

        private static bool isFilterableNode2(SingleValueNode node, bool notIsApplied)
        {
            if (node is UnaryOperatorNode)
            {
                // If NOT, negate tree and return isFilterable
                UnaryOperatorNode asUONode = node as UnaryOperatorNode;
                if (asUONode.OperatorKind == Microsoft.Data.OData.Query.UnaryOperatorKind.Not)
                {
                    return isFilterableNode2(asUONode.Operand, !notIsApplied);
                }
                else
                {
                    throw new NotSupportedException("Does not support the Negate unary operator");
                }
            }
            else if (node is BinaryOperatorNode)
            {
                // Filterable(A) AND Filterable(B)      => Intersect(Filter(A), Filter(B))
                // !Filterable(A) AND Filterable(B)     => Filter(B)
                // Filterable(A) AND !Filterable(B)     => Filter(A)
                // !Filterable(A) AND !Filterable(B)    => NF
                BinaryOperatorNode asBONode = node as BinaryOperatorNode;
                if ((asBONode.OperatorKind == BinaryOperatorKind.And && !notIsApplied) ||
                    (asBONode.OperatorKind == BinaryOperatorKind.Or && notIsApplied))
                {
                    if (!isFilterableNode2(asBONode.Left, notIsApplied) && !isFilterableNode2(asBONode.Right, notIsApplied))
                    {
                        return false;
                    }

                    return true;
                }
                // Filterable(A) OR Filterable(B)      => Union(Filter(A), Filter(B))
                // !Filterable(A) OR Filterable(B)     => NF
                // Filterable(A) OR !Filterable(B)     => NF
                // !Filterable(A) OR !Filterable(B)    => NF
                else if ((asBONode.OperatorKind == BinaryOperatorKind.Or && !notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.And && notIsApplied))
                {
                    if (isFilterableNode2(asBONode.Left, notIsApplied) && isFilterableNode2(asBONode.Right, notIsApplied))
                    {
                        return true;
                    }

                    return false;
                }
                // If Equals, >=, >, <, <=
                else if ((asBONode.OperatorKind == BinaryOperatorKind.Equal && !notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.NotEqual && notIsApplied) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.GreaterThan) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.GreaterThanOrEqual) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.LessThan) ||
                         (asBONode.OperatorKind == BinaryOperatorKind.LessThanOrEqual))
                {
                    return true;
                }
                // We choose to mark NotEquals as unfilterable. Theoretically with indexes with low number of keys may be slightly faster than not-filtering
                // But generally is same order of magnitude as not using filters
                else if ((asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.Equal && notIsApplied) ||
                         (asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.NotEqual && !notIsApplied))
                {
                    return false;
                }
                else
                {
                    throw new NotSupportedException("Does not support Add, Subtract, Modulo, Multiply, Divide operations.");
                }
            }
            else if (node is ConvertNode)
            {
                ConvertNode asCNode = node as ConvertNode;
                return isFilterableNode2(asCNode.Source, notIsApplied);
            }
            else
            {
                throw new NotSupportedException("Only supports Binary and Unary operator nodes");
            }
        } 

        /// <summary>
		/// Query the reliable collection with the given name from the reliable state manager using the given query parameters.
		/// </summary>
		/// <returns>A tuple with first element as name of parameter and second the value to compare against</returns>
        private static ConditionalValue<Tuple<string, object>> IsFilterableQuery(ODataQueryOptions options)
        {
            if (options.Filter == null)
            {
                return new ConditionalValue<Tuple<string, object>>(false, null);
            }
            FilterClause clause = options.Filter.FilterClause;
            return isFilterableNode(clause.Expression, false);
        }

        // If node is a not node, much behavior changes
        private static ConditionalValue<Tuple<string, object>> isFilterableNode(SingleValueNode node, bool isNot)
        {
            if (node is UnaryOperatorNode)
            {
                // If NOT, negate tree and return isFilterable
                UnaryOperatorNode asUONode = node as UnaryOperatorNode;
                if (asUONode.OperatorKind == Microsoft.Data.OData.Query.UnaryOperatorKind.Not)
                {
                    return isFilterableNode(asUONode.Operand, !isNot);
                }
            }
            else if (node is BinaryOperatorNode)
            {
                // If AND, if either node isFilterable, then node is filterable (not makes AND act like OR)
                BinaryOperatorNode asBONode = node as BinaryOperatorNode;
                if ((asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.And && !isNot) ||
                    (asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.Or && isNot))
                {
                    ConditionalValue<Tuple<string, object>> isLeftFilterable = isFilterableNode(asBONode.Left, isNot);
                    if (isLeftFilterable.HasValue) { return isLeftFilterable; }
                    ConditionalValue<Tuple<string, object>> isRightFilterable = isFilterableNode(asBONode.Right, isNot);
                    if (isRightFilterable.HasValue) { return isRightFilterable; }
                    return new ConditionalValue<Tuple<string, object>>(false, null);

                }
                // If OR, node is not filterable
                // TODO, if both are filterable, can filter both and union results
                else if ((asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.Or && !isNot) ||
                         (asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.And && isNot))
                {
                    return new ConditionalValue<Tuple<string, object>>(false, null);
                }
                // If Equal, should be filterable, find pertinent info
                else if ((asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.Equal && !isNot) ||
                         (asBONode.OperatorKind == Microsoft.Data.OData.Query.BinaryOperatorKind.NotEqual && isNot))
                {
                    if (asBONode.Left is SingleValuePropertyAccessNode)
                    {
                        if (asBONode.Right is ConstantNode)
                        {
                            SingleValuePropertyAccessNode asSVPANode = asBONode.Left as SingleValuePropertyAccessNode;
                            ConstantNode asCNode = asBONode.Right as ConstantNode;

                            return new ConditionalValue<Tuple<string, object>>(true, new Tuple<string, object>(asSVPANode.Property.Name, asCNode.Value));
                        }
                    }
                    else if (asBONode.Left is ConstantNode)
                    {
                        if (asBONode.Right is SingleValuePropertyAccessNode)
                        {
                            SingleValuePropertyAccessNode asSVPANode = asBONode.Right as SingleValuePropertyAccessNode;
                            ConstantNode asCNode = asBONode.Left as ConstantNode;

                            return new ConditionalValue<Tuple<string, object>>(true, new Tuple<string, object>(asSVPANode.Property.Name, asCNode.Value));
                        }
                    }
                }
            }
            return new ConditionalValue<Tuple<string, object>>(false, null);
        }

        /// <summary>
        /// This implementation should not leak into the core Queryable code.  It is dependent on the specific protocol
        /// used to communicate with the other partitions (HTTP over ReverseProxy), and should be hidden behind an interface.
        /// </summary>
        private static async Task<IEnumerable<JToken>> QueryPartitionAsync(Partition partition,
			StatefulServiceContext context, string collection, IEnumerable<KeyValuePair<string, string>> query, CancellationToken cancellationToken)
		{
			string endpoint = await StatefulServiceUtils.GetPartitionEndpointAsync(context, partition).ConfigureAwait(false);
			string content = await StatefulServiceUtils.QueryPartitionAsync(endpoint, partition.PartitionInformation.Id, collection, query).ConfigureAwait(false);

			var result = JsonConvert.DeserializeObject<ODataResult>(content);
			return result.Value;
		}

		/// <summary>
		/// Execute the operations given in <paramref name="operations"/> in a transaction.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="operations">Operations (add/update/delete) to perform against collections in the partition.</param>
		/// <returns>A list of status codes indicating success/failure of the operations.</returns>
		public static async Task<List<EntityOperationResult>> ExecuteAsync(this IReliableStateManager stateManager, EntityOperation<JToken, JToken>[] operations)
		{
			var results = new List<EntityOperationResult>();
			using (var tx = stateManager.CreateTransaction())
			{
				bool commit = true;
				foreach (var operation in operations)
				{
					HttpStatusCode status = HttpStatusCode.BadRequest;
					string description = null;

					try
					{
						// Get the reliable dictionary for this operation.
						var dictionary = await stateManager.GetQueryableState(operation.Collection).ConfigureAwait(false);

						// Execute operation.
						if (operation.Operation == Operation.Add)
						{
							status = await ExecuteAddAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
						else if (operation.Operation == Operation.Update)
						{
							status = await ExecuteUpdateAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
						else if (operation.Operation == Operation.Delete)
						{
							status = await ExecuteDeleteAsync(tx, dictionary, operation).ConfigureAwait(false);
						}
					}
					catch (QueryException e)
					{
						status = e.Status;
						description = e.Message;
					}
					catch (ArgumentException e)
					{
						status = HttpStatusCode.BadRequest;
						description = e.Message;
					}
					catch (Exception)
					{
						status = HttpStatusCode.InternalServerError;
					}

					// Add the operation result.
					results.Add(new EntityOperationResult
					{
						PartitionId = operation.PartitionId,
						Collection = operation.Collection,
						Key = operation.Key,
						Status = (int)status,
						Description = description,
					});

					// If any operation failed, abort the transaction.
					if (!status.IsSuccessStatusCode())
						commit = false;
				}

				// Commit or abort the transaction.
				if (commit)
					await tx.CommitAsync().ConfigureAwait(false);
				else
					tx.Abort();
			}

			return results;
		}

		private static async Task<HttpStatusCode> ExecuteAddAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			Type keyType = dictionary.GetKeyType();
			Type valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			var value = operation.Value.ToObject(valueType);
			Type dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Add to reliable dictionary.
			MethodInfo tryAddMethod = dictionaryType.GetMethod("TryAddAsync", new[] { typeof(ITransaction), keyType, valueType });
			bool success = await ((Task<bool>)tryAddMethod.Invoke(dictionary, new[] { tx, key, value })).ConfigureAwait(false);
			if (!success)
				throw new QueryException(HttpStatusCode.Conflict, "Key already exists.");

			return HttpStatusCode.OK;
		}

		private static async Task<HttpStatusCode> ExecuteUpdateAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			Type keyType = dictionary.GetKeyType();
			Type valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			var value = operation.Value.ToObject(valueType);
			Type dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Read the existing value.
			MethodInfo tryGetMethod = dictionaryType.GetMethod("TryGetValueAsync", new[] { typeof(ITransaction), keyType, typeof(LockMode) });
			var tryGetTask = (Task)tryGetMethod.Invoke(dictionary, new[] { tx, key, LockMode.Update });
			await tryGetTask.ConfigureAwait(false);
			var tryGetResult = tryGetTask.GetPropertyValue<object>("Result");

			// Only update the value if it exists.
			if (!tryGetResult.GetPropertyValue<bool>("HasValue"))
				throw new QueryException(HttpStatusCode.NotFound, "Key not found.");

			// Validate the ETag.
			var currentValue = tryGetResult.GetPropertyValue<object>("Value");
			var currentEtag = CRC64.ToCRC64(JsonConvert.SerializeObject(currentValue)).ToString();
			if (currentEtag != operation.Etag)
				throw new QueryException(HttpStatusCode.PreconditionFailed, "The value has changed on the server.");

			// Update in reliable dictionary.
			MethodInfo setMethod = dictionaryType.GetMethod("SetAsync", new[] { typeof(ITransaction), keyType, valueType });
			await ((Task)setMethod.Invoke(dictionary, new[] { tx, key, value })).ConfigureAwait(false);

			return HttpStatusCode.OK;
		}

		private static async Task<HttpStatusCode> ExecuteDeleteAsync(ITransaction tx, IReliableState dictionary, EntityOperation<JToken, JToken> operation)
		{
			// Get type information.
			Type keyType = dictionary.GetKeyType();
			Type valueType = dictionary.GetValueType();
			var key = operation.Key.ToObject(keyType);
			Type dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(keyType, valueType);

			// Read the existing value.
			MethodInfo tryGetMethod = dictionaryType.GetMethod("TryGetValueAsync", new[] { typeof(ITransaction), keyType, typeof(LockMode) });
			var tryGetTask = (Task)tryGetMethod.Invoke(dictionary, new[] { tx, key, LockMode.Update });
			await tryGetTask.ConfigureAwait(false);
			var tryGetResult = tryGetTask.GetPropertyValue<object>("Result");

			// Only update the value if it exists.
			if (!tryGetResult.GetPropertyValue<bool>("HasValue"))
				throw new QueryException(HttpStatusCode.NotFound, "Key not found.");

			// Validate the ETag.
			var currentValue = tryGetResult.GetPropertyValue<object>("Value");
			var currentEtag = CRC64.ToCRC64(JsonConvert.SerializeObject(currentValue)).ToString();
			if (currentEtag != operation.Etag)
				throw new QueryException(HttpStatusCode.PreconditionFailed, "The value has changed on the server.");

			// Delete from reliable dictionary.
			MethodInfo tryDeleteMethod = dictionaryType.GetMethod("TryRemoveAsync", new[] { typeof(ITransaction), keyType });
			await ((Task)tryDeleteMethod.Invoke(dictionary, new[] { tx, key })).ConfigureAwait(false);

			return HttpStatusCode.OK;
		}

		/// <summary>
		/// Get the queryable reliable collection by name.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="collection">Name of the reliable collection.</param>
		/// <returns>The reliable collection that supports querying.</returns>
		private static async Task<IReliableState> GetQueryableState(this IReliableStateManager stateManager, string collection)
		{
			// Find the reliable state.
			var reliableStateResult = await stateManager.TryGetAsync<IReliableState>(collection).ConfigureAwait(false);
			if (!reliableStateResult.HasValue)
				throw new ArgumentException($"IReliableState '{collection}' not found.");

			// Verify the state is a reliable dictionary.
			var reliableState = reliableStateResult.Value;
			if (!reliableState.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException($"IReliableState '{collection}' must be an IReliableDictionary.");

			return reliableState;
		}

		/// <summary>
		/// Get the names and types of the reliable collections that are queryable from the reliable state manager.
		/// </summary>
		/// <param name="stateManager">Reliable state manager for the replica.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The names and value types of the reliable collections that are queryable.</returns>
		private static async Task<IEnumerable<KeyValuePair<string, Type>>> GetQueryableTypes(
			this IReliableStateManager stateManager, CancellationToken cancellationToken = default(CancellationToken))
		{
			var types = new Dictionary<string, Type>();
			var enumerator = stateManager.GetAsyncEnumerator();
			while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
			{
				var state = enumerator.Current;
				if (state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				{
					var entityType = state.GetEntityType();
					types.Add(state.Name.AbsolutePath, entityType);
				}
			}

			return types;
		}

		/// <summary>
		/// Get the Entity model type from the reliable dictionary.
		/// This is the full metadata type definition for the rows in the
		/// dictionary (key, value, partition, etag).
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The Entity model type for the dictionary.</returns>
		private static Type GetEntityType(this IReliableState state)
		{
			var keyType = state.GetKeyType();
			var valueType = state.GetValueType();
			return typeof(Entity<,>).MakeGenericType(keyType, valueType);
		}

		/// <summary>
		/// Get the key type from the reliable dictionary.
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The type of the dictionary's keys.</returns>
		private static Type GetKeyType(this IReliableState state)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			return state.GetType().GetGenericArguments()[0];
		}

		/// <summary>
		/// Get the value type from the reliable dictionary.
		/// </summary>
		/// <param name="state">Reliable dictionary instance.</param>
		/// <returns>The type of the dictionary's values.</returns>
		private static Type GetValueType(this IReliableState state)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			return state.GetType().GetGenericArguments()[1];
		}

		/// <summary>
		/// Gets the values from the reliable state as the <see cref="Entity{TKey, TValue}"/> of the collection.
		/// </summary>
		/// <param name="state">Reliable state (must implement <see cref="IReliableDictionary{TKey, TValue}"/>).</param>
		/// <param name="tx">Transaction to create the enumerable under.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>Values from the reliable state as <see cref="Entity{TKey, TValue}"/> values.</returns>
		private static async Task<IAsyncEnumerable<object>> GetAsyncEnumerable(this IReliableState state,
			ITransaction tx, IReliableStateManager stateManager, Guid partitionId, CancellationToken cancellationToken)
		{
			if (!state.ImplementsGenericType(typeof(IReliableDictionary<,>)))
				throw new ArgumentException(nameof(state));

			var entityType = state.GetEntityType();

			// Create the async enumerable.
			var dictionaryType = typeof(IReliableDictionary<,>).MakeGenericType(state.GetType().GetGenericArguments());
			var createEnumerableAsyncTask = state.CallMethod<Task>("CreateEnumerableAsync", new[] { typeof(ITransaction) }, tx);
			await createEnumerableAsyncTask.ConfigureAwait(false);

			// Get the AsEntity method to convert to an Entity enumerable.
			var asyncEnumerable = createEnumerableAsyncTask.GetPropertyValue<object>("Result");
			var asEntityMethod = typeof(ReliableStateExtensions).GetMethod("AsEntity", BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(entityType.GenericTypeArguments);
			return (IAsyncEnumerable<object>)asEntityMethod.Invoke(null, new object[] { asyncEnumerable, partitionId, cancellationToken });
		}

		/// <summary>
		/// Lazily convert the reliable state enumerable into a queryable <see cref="Entity{TKey, TValue}"/> enumerable.
		/// /// </summary>
		/// <param name="source">Reliable state enumerable.</param>
		/// <param name="partitionId">Partition id.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>The</returns>
		private static IAsyncEnumerable<Entity<TKey, TValue>> AsEntity<TKey, TValue>(this IAsyncEnumerable<KeyValuePair<TKey, TValue>> source, Guid partitionId, CancellationToken cancellationToken)
		{
			return source.SelectAsync(kvp => new Entity<TKey, TValue>
			{
				PartitionId = partitionId,
				Key = kvp.Key,
				Value = kvp.Value,

				// TODO: only set the ETag if the query selects this object.
				Etag = CRC64.ToCRC64(JsonConvert.SerializeObject(kvp.Value)).ToString(),
			});
		}

		/// <summary>
		/// Apply the OData query specified by <paramref name="query"/> to the in-memory objects.
		/// </summary>
		/// <param name="data">The in-memory objects to query.</param>
		/// <param name="type">The Type of the objects in <paramref name="data"/>.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="aggregate">Indicates whether this is an aggregation or partial query.</param>
		/// <returns>The results of applying the query to the in-memory objects.</returns>
		private static IEnumerable<object> ApplyQuery(IEnumerable<object> data, Type type,
			IEnumerable<KeyValuePair<string, string>> query, bool aggregate)
		{
			// Get the OData query context for this type.
			var context = QueryCache.GetQueryContext(type);

			// Cast to correct IQueryable type.
			var casted = data.CastEnumerable(type);

			// Execute the query.
			var options = new ODataQueryOptions(query, context, aggregate);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			var result = options.ApplyTo(casted.AsQueryable(), settings);

			// Get the query results.
			return result.Cast<object>();
		}

		/// <summary>
		/// Apply the OData query specified by <paramref name="query"/> to the objects.
		/// </summary>
		/// <param name="data">The objects to query.</param>
		/// <param name="type">The Type of the objects in <paramref name="data"/>.</param>
		/// <param name="query">OData query parameters.</param>
		/// <param name="aggregate">Indicates whether this is an aggregation or partial query.</param>
		/// <returns>The results of applying the query to the in-memory objects.</returns>
		private static IAsyncEnumerable<object> ApplyQuery(IAsyncEnumerable<object> data, Type type,
			IEnumerable<KeyValuePair<string, string>> query, bool aggregate)
		{
			// Get the OData query context for this type.
			var context = QueryCache.GetQueryContext(type);

			// Execute the query.
			var options = new ODataQueryOptions(query, context, aggregate);
			var settings = new ODataQuerySettings { HandleNullPropagation = HandleNullPropagationOption.True };
			return options.ApplyTo(data, settings);
		}
	}
}