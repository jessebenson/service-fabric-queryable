using Microsoft.ServiceFabric.Services.Queryable;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Web.Http;

namespace Basic.WebSvc.Controllers
{
	[ServiceRequestActionFilter]
	public class QueryController : QueryableController
	{
		[HttpGet] // GET query/{application}/{service}/$metadata
		[Route("query/{application}/{service}/$metadata")]
		public Task<IHttpActionResult> GetMetadata(string application, string service)
		{
			return base.GetMetadataAsync(application, service);
		}

		[HttpGet] // GET query/{application}/{service}/{collection}?<OData query>
		[Route("query/{application}/{service}/{collection}")]
		public Task<IHttpActionResult> Query(string application, string service, string collection)
		{
			return base.QueryAsync(application, service, collection);
		}
	}
}
