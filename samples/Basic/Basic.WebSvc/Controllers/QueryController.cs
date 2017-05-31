using Microsoft.ServiceFabric.Services.Queryable;
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
		[HttpGet]
		[Route("query/{application}/{service}/$metadata")]
		public Task<IHttpActionResult> GetMetadata(string application, string service)
		{
			return base.GetMetadataAsync(application, service);
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
		[HttpGet]
		[Route("query/{application}/{service}/{collection}")]
		public Task<IHttpActionResult> Query(string application, string service, string collection)
		{
			return base.QueryAsync(application, service, collection);
		}
	}
}
