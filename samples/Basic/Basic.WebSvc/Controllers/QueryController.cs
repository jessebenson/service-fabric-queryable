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
		/// Adds appropriate key and corresponding value to the given reliable collection in the queryable service. If it is already existing a bad request exception is raised.
		/// SINGLE ADD:
		/// - POST /query/BasicApp/ProductSvc/products and in Body provide a Json:
		/// In Body: [{
		///	        "Key": "sku-218",
		///	        "Value":  {
		///	            "Sku": "sku-218",
		///	            "Price": 10.95,
		///	            "Quantity":46
		///	        },
		///	        "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
		///	    }
		///	    ]
		/// Record belonging to the key provided in the JSON Body of HTTP POST Request is added to a partition ID mentioned, if its not existing already.
		/// Incase Partition ID is not mentioned, Record is added to random partition ID. If it is already existing a bad request exception is raised.
		///
		/// BATCH ADD (Provide an Array of keys & values with optional partitionID in JSON format inside body of HTTP POST request to add them all.).
		/// -POST /query/BasicApp/ProductSvc/products
		/// In Body: [{
		///             "Key": "sku-218",
		///             "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
		///           },{
		///             "Key": "sku-217",
		///             "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
		///          }]
		///
		/// ADD a Key & Value to a random Partition:
		/// -POST /query/BasicApp/ProductSvc/products
		/// In Body :
		/// [{
		///"Key": "sku-218"
		///}]
		///
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