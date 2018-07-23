# Microsoft.ServiceFabric.Services.Queryable

Enable Data Explorer and query support for your stateful services in Service Fabric via the OData protocol.  I am actively working on integrating the Data Explorer into the Service Fabric Explorer.  Below is a preview (not yet available) of what it will look like:

![](images/data-explorer-query.png)

Add the ODataQueryable middleware to your stateful services (using ASP.NET Core stateful services), ensure Reverse Proxy is enabled, and start querying your reliable collections.  If your service is named 'fabric:/MyApp/MyService' and your reliable dictionary is named 'my-dictionary', try queries like:

Get OData metadata about a single partition stateful service:
- ```GET http://localhost:19081/MyApp/MyService/$query/$metadata```

Get OData metadata about a partitioned stateful service:
- ```GET http://localhost:19081/MyApp/MyService/$query/$metadata?PartitionKind=Int64Range&PartitionKey=0```

Get 10 items from the reliable dictionary.
- ```GET http://localhost:19081/MyApp/MyService/$query/my-dictionary?$top=10```

Get 10 items with Quantity between 2 and 4, inclusively.
- ```GET http://localhost:19081/MyApp/MyService/$query/my-dictionary?$top=10&$filter=Quantity ge 2 and Quantity le 4```

Get 10 items, returning only the Price and Quantity properties, sorted by Price in descending order.
- ```GET http://localhost:19081/MyApp/MyService/$query/my-dictionary?$top=10&$select=Price,Quantity&$orderby=Price desc```

## Use ReliableIndexedDictionaries to speed up your queries by taking advantage of secondary indices
Some queries may take a long time or not complete. If you expect that you will be querying against a field often, you should consider adding a secondary index on that field, which will dramatically speed up your queries.

To do so, when you are making your dictionary, instead of using 

```StateManager.GetOrAddAsync<IReliableDictionary>("name")```

you should use 

```
StateManager.GetOrAddIndexedAsync("name",
	FilterableIndex<KeyType, ValueType, Property1Type>.CreateQueryableInstance("Property1Name"),
	FilterableIndex<KeyType, ValueType, Property2Type>.CreateQueryableInstance("Property2Name"),
	etc.)
```

This will create a dictionary with secondary indices on `ValueType.Property1Name` and `ValueType.Property2Name`. To find out more about ReliableIndexedDictionary go to the [indexing repository](https://github.com/jessebenson/service-fabric-indexing)

Now, if we made a secondary index on a property called `Age` on our `ValueType`, these queries would be faster because of indexing:
- ```$filter=Value.Age eq 20```
- ```$filter=Value.Age gt 25```
- ```$filter=Value.Age le 40 and Value.Name eq "John"```

However the following would not be faster than without using an indexed dictionary:
- ```$filter=Value.Name eq "John"```
- ```$filter=Value.Age le 40 or Value.Name eq "John"```

If we added a secondary index on both `Age` and `Name`, then all the above queries would be faster.

Check out [`UserSvc`](samples/Basic/Basic.UserSvc/UserSvc.cs) in the  [BasicApp sample](samples/Basic) to see both an unindexed and an indexed dictionary being constructed.

## Using LINQ to query ReliableIndexedDictionaries
In addition to external requests through the OData middleware, ReliableIndexedDictionaries can be queried from your application code using LINQ. However, similarly to external queries, not all queries will work effectively with indexing. If your query is not supported, you should use `GetAllAsync()` to get the entire result set and apply your LINQ query against that.

To create a queryable instance of your IReliableIndexedDictionary, call:
```csharp
var qd = new QueryableReliableIndexedDictionary<TKey, TValue, TValue>(indexedDictionary, stateManager);
```
Then you can carry out queries such as:
```csharp

var query = qd.Where(x => x.Age == 25);
var query = qd.Where(x => x.Age >= 25).Select(x => x.Email);
var query = from Person person in qd
            where person.Age >= 25 && person.Email == "user-0@example.com"
            select person.Email;
var query = qd.Where(x => x.Name.CompareTo("Name1") >= 0);
```
Some import notes for querying:
1. Put all your WHERE logic in a single `Where` statement and join using `&&` and `||` operators, as the query may not be efficient if it is spread across multiple WHERE clauses.
2. If you want to compare an IComparable type that does not have ">", "<=" operators, you must give it in the form: `property.CompareTo(constant) '>,<,<=,=>' 0` 


## Getting Started

1. Create a stateful ASP.NET Core services.

2. Add the **Microsoft.ServiceFabric.Services.Queryable** nuget package.

3. Add the **ODataQueryable** middleware to your Startup.cs.  This will intercept calls to the /$query endpoint to expose OData query capabilities over the reliable collections in your service.

```csharp
using Microsoft.ServiceFabric.Services.Queryable;

public class Startup
{
	public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
	{
		...
		app.UseODataQueryable();
		...
	}
}
```

## Samples

- [Basic application with several stateful services](samples/Basic)
