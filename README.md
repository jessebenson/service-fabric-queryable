# Microsoft.ServiceFabric.Services.Queryable

Enable query support for your stateful services in Service Fabric via the OData protocol.

Derive from **QueryableService** from your stateful services, expose the required HTTP APIs, and start querying your reliable collections.  If your service is named 'fabric:/MyApp/MyService' and your reliable dictionary is named 'my-dictionary', try queries like:

Get OData metadata about a stateful service:
- ```GET http://localhost/query/MyApp/MyService/$metadata```

Get 10 items from the reliable dictionary.
- ```GET http://localhost/query/MyApp/MyService/my-dictionary?$top=10```

Get 10 items with Quantity between 2 and 4, inclusively.
- ```GET http://localhost/query/MyApp/MyService/my-dictionary?$top=10&$filter=Quantity ge 2 and Quantity le 4```

Get 10 items, returning only the Price and Quantity properties, sorted by Price in descending order.
- ```GET http://localhost/query/MyApp/MyService/my-dictionary?$top=10&$select=Price,Quantity&$orderby=Price desc```

## Getting Started

1. Add the **Microsoft.ServiceFabric.Services.Queryable** nuget package.

2. Derive your stateful service from **QueryableService** instead of StatefulService, and expose a service replica listener.  This will implement the **IQueryableService** service remoting interface to expose OData query capabilities over the reliable collections in your service.

```csharp
using Microsoft.ServiceFabric.Services.Queryable;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;

internal sealed class ProductSvc : QueryableService
{
	protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
	{
		return new[]
		{
			new ServiceReplicaListener(this.CreateServiceRemotingListener),
		};
	}
}
```

3. Add an ApiController that derives from **QueryableController**, and implement the two required methods.

```csharp
using Microsoft.ServiceFabric.Services.Queryable;

public class QueryController : QueryableController
{
	/// <summary>
	/// Returns OData metadata about the queryable reliable collections and types in the application/service.
	/// </summary>
	[HttpGet]
	[Route("query/{application}/{service}/$metadata")]
	public Task<IHttpActionResult> GetMetadata(string application, string service)
	{
		return base.GetMetadataAsync(application, service);
	}

	/// <summary>
	/// Queries the given reliable collection in the queryable service using the OData query language.
	/// </summary>
	[HttpGet]
	[Route("query/{application}/{service}/{collection}")]
	public Task<IHttpActionResult> Query(string application, string service, string collection)
	{
		return base.QueryAsync(application, service, collection);
	}
}
```

## Samples

- [Basic application with a stateless front-end and partitioned stateful Products service](samples/Basic)
