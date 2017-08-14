using Microsoft.ServiceFabric.Services.Queryable;
using Microsoft.ServiceFabric.Services.Queryable.Controller;
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
		/// and types in the application/service/Specific Partition Id.  If the service name
		/// is 'fabric:/MyApp/MyService', & PartitionId is MyPid then the HTTP Uri should be formatted as:
		///
		/// - GET query/MyApp/MyService/MyPid/$metadata
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/{partitionId}/$metadata")]
		public Task<IHttpActionResult> GetPartitionMetadata(string application, string service, string partitionId)
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
		/// Queries the given reliable collection in the queryable service for the specific partitionId
		/// using the OData query language. Example queries:
		///
		/// Get 10 items from the reliable dictionary named 'my-dictionary' in the service named 'fabric:/MyApp/MyService' with PartitionId as MyPid.
		/// - GET query/MyApp/MyService/MyPid/my-dictionary?$top=10
		///
		/// Get 10 items with Quantity between 2 and 4, inclusively.
		/// - GET query/MyApp/MyService/MyPid/my-dictionary?$top=10&$filter=Quantity ge 2 and Quantity le 4
		///
		/// Get 10 items, returning only the Price and Quantity properties, sorted by Price in descending order.
		/// - GET query/MyApp/MyService/MyPid/my-dictionary?$top=10&$select=Price,Quantity&$orderby=Price desc
		/// </summary>
		[HttpGet, HttpOptions]
		[Route("query/{application}/{service}/{partitionId}/{collection}")]
		public Task<IHttpActionResult> QueryPartition(string application, string service, string partitionId, string collection)
		{
			return QuerySpecificPartitionAsync(application, service, partitionId, collection);
		}

		/// <summary>
		/// Does all listed Dml operations in a single transaction. Atomic in nature.
		/// Sample:
		/// - POST /query/BasicApp/ProductSvc and in Body provide a Json Array:
		/// In Body: [{
		//	"Operation":"Update",
		//	"Collection":"products",
		//            "PartitionId": "086da0f0-1382-4314-b1f5-d0a72863ffb1",
		//            "Key": "sku-1",
		//            "Value": {
		//                "Sku": "sku-UPDATE",
		//                "Price": 110,
		//                "Quantity": 9999
		//            }
		//},
		//{
		//	"Operation":"Delete",
		//	"Collection":"products2",
		//	"PartitionId": "8cb6e02d-c7ba-4d72-a329-ffb4d2cc4f7a",
		//            "Key": "sku-0",
		//            "Value": {
		//                "Sku": "sku-0",
		//                "Price": 10,
		//                "Quantity": 0
		//            },
		//	},
		//{
		//	"Operation":"Add",
		//	"Collection":"products",
		//	"PartitionId": "8cb6e02d-c7ba-4d72-a329-ffb4d2cc4f7a",
		//            "Key": "sku-new",
		//            "Value": {
		//                "Sku": "sku-0",
		//                "Price": 10,
		//                "Quantity": 0
		//            }
		//}
		///	    ]
		/// Record belonging to the key provided in the JSON Body of HTTP POST Request is added to a partition ID mentioned, if its not existing already.
		/// Incase Partition ID is not mentioned, Record is added to random partition ID. If it is already existing a bad request exception is raised.
		/// </summary>
		[HttpPost, HttpOptions]
		[Route("query/{application}/{service}")]
		public Task<IHttpActionResult> Dml(string application, string service,
				[FromBody] ValueViewModel[] obj)
		{
			return base.DmlAsync(application, service, obj);
		}
	}
}