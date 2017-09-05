using Microsoft.ServiceFabric.Services.Queryable;
using Microsoft.ServiceFabric.Services.Queryable.Controller;
using System;
using System.Threading.Tasks;
using System.Web.Http;

namespace Basic.WebSvc.Controllers
{
	public class QueryController : QueryableController
	{
		/// <summary>
		/// Returns OData metadata about the queryable reliable collections
		/// and types in the application/service.  If the service name
		/// is 'fabric:/MyApp/MyService', the HTTP Uri should be formatted as:
		///
		/// - GET query/MyApp/MyService/$metadata
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/$metadata")]
		public Task<IHttpActionResult> GetMetadata(string application, string service)
		{
			return GetMetadataAsync(application, service);
		}

		/// <summary>
		/// Returns OData metadata about the queryable reliable collections
		/// and types in the application/service/partition.  If the service name
		/// is 'fabric:/MyApp/MyService' and the is 01234567-8900-0000-0000-abcdef000000, 
		/// the HTTP Uri should be formatted as:
		///
		/// - GET query/MyApp/MyService/01234567-8900-0000-0000-ABCDEF000000/$metadata
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/{partitionId}/$metadata")]
		public Task<IHttpActionResult> GetPartitionMetadata(string application, string service, Guid partitionId)
		{
			return GetPartitionMetadataAsync(application, service, partitionId);
		}

		/// <summary>
		/// Queries the given reliable collection in the queryable service
		/// using the OData query language. Example queries:
		///
		/// Get 10 items from the reliable dictionary named 'my-dictionary' in the service named 'fabric:/MyApp/MyService'.
		/// - GET query/MyApp/MyService/my-dictionary?$top=10
		///
		/// Get 10 items with Quantity between 2 and 4, inclusively.
		/// - GET query/MyApp/MyService/my-dictionary?$top=10&$filter=Quantity ge 2 and Quantity le 4
		///
		/// Get 10 items, returning only the Price and Quantity properties, sorted by Price in descending order.
		/// - GET query/MyApp/MyService/my-dictionary?$top=10&$select=Price,Quantity&$orderby=Price desc
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/{collection}")]
		public Task<IHttpActionResult> Query(string application, string service, string collection)
		{
			return QueryAsync(application, service, collection);
		}

		/// <summary>
		/// Queries the given reliable collection in the queryable service for the specific partition
		/// using the OData query language. Example queries:
		///
		/// Get 10 items from the reliable dictionary named 'my-dictionary' in the service named 'fabric:/MyApp/MyService' with partition 01234567-8900-0000-0000-abcdef000000.
		/// - GET query/MyApp/MyService/01234567-8900-0000-0000-abcdef000000/my-dictionary?$top=10
		///
		/// Get 10 items with Quantity between 2 and 4, inclusively.
		/// - GET query/MyApp/MyService/01234567-8900-0000-0000-abcdef000000/my-dictionary?$top=10&$filter=Quantity ge 2 and Quantity le 4
		///
		/// Get 10 items, returning only the Price and Quantity properties, sorted by Price in descending order.
		/// - GET query/MyApp/MyService/01234567-8900-0000-0000-abcdef000000/my-dictionary?$top=10&$select=Price,Quantity&$orderby=Price desc
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/{partitionId}/{collection}")]
		public Task<IHttpActionResult> QueryPartition(string application, string service, Guid partitionId, string collection)
		{
			return QuerySpecificPartitionAsync(application, service, partitionId, collection);
		}

		/// <summary>
		/// Does all listed Dml operations in a single transaction. Atomic in nature.
		/// Sample:
		/// - POST /query/BasicApp/ProductSvc and in Body provide a Json Array:
		/// In Body:
		/// [{
		///    "Operation": "Update",
		///    "Collection": "products",
		///    "PartitionId": "01234567-8900-0000-0000-abcdef000000",
		///    "Key": "sku-1",
		///    "Value": {
		///      "Sku": "sku-1",
		///      "Price": 110,
		///      "Quantity": 9999
		///    }
		///  },
		///  {
		///    "Operation": "Delete",
		///    "Collection": "products2",
		///    "PartitionId": "01234567-8900-0000-0000-abcdef000000",
		///    "Key": "sku-0"
		///  },
		///  {
		///    "Operation": "Add",
		///    "Collection": "products",
		///    "PartitionId": "01234567-8900-0000-0000-abcdef000000",
		///    "Key": "sku-new",
		///    "Value": {
		///      "Sku": "sku-new",
		///      "Price": 10,
		///      "Quantity": 0
		///    }
		/// }]
		/// 
		/// Record belonging to the key provided in the JSON Body of HTTP POST Request is added to a partition ID mentioned, if its not existing already.
		/// Incase Partition ID is not mentioned, Record is added to random partition ID. If it is already existing a bad request exception is raised.
		/// </summary>
		[HttpPost, HttpOptions]
		[Route("query/{application}/{service}")]
		public Task<IHttpActionResult> Dml(string application, string service, [FromBody] ValueViewModel[] obj)
		{
			return DmlAsync(application, service, obj);
		}
	}
}