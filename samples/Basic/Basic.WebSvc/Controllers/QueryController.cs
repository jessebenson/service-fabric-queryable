using Microsoft.ServiceFabric.Services.Queryable;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.ServiceFabric.Services.Queryable.Controller;
using Newtonsoft.Json.Linq;

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
		[HttpGet]
		[Route("query/{application}/{service}/{collection}")]
		public Task<IHttpActionResult> Query(string application, string service, string collection)
		{
			return base.QueryAsync(application, service, collection);
        }


        /// <summary>
        /// Deletes appropriate key & corresponding value from the given reliable collection in the queryable service.
        /// SINGLE DELETE:
        /// - DELETE /query/BasicApp/ProductSvc/products and in Body provide a Json:
        /// In Body: [{
	    ///"Key": "sku-218",
	    /// "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
	    ///}]
	    /// Record belonging to the key provided in the JSON Body of HTTP Request is located in the given partitionID and removed from it.
	    /// 
	    /// BATCH DELETE (Provide an Array of keys in JSON format inside body of HTTP Delete request to delete them all (with kindness ;) ). 
        /// -DELETE /query/BasicApp/ProductSvc/products
        /// In Body: [{
        ///"Key": "sku-218",
        /// "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
        ///},{
        ///"Key": "sku-217",
        /// "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
        ///}]
        /// 
        /// DELETE A Key from All Partitions :
        /// -DELETE /query/BasicApp/ProductSvc/products 
        /// In Body : 
        /// [{
        ///"Key": "sku-218"
        ///}]
        /// 
        /// </summary>

        [HttpDelete]
		[Route("query/{application}/{service}/{collection}")]
		public Task<IHttpActionResult> DeleteAsync(string application, string service, string collection, [FromBody] ValueViewModel[] obj)
		{
			return base.DeleteAsync(application, service, collection, obj);
        }
	    /// <summary>
	    /// Adds appropriate key & corresponding value to the given reliable collection in the queryable service.
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
	    /// Incase Partition ID is not mentioned, Record is added to random partition ID.
	    /// 
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
	    [HttpPost]
        [Route("query/{application}/{service}/{collection}")]
	    public Task<IHttpActionResult> AddAsync(string application, string service, string collection, [FromBody] ValueViewModel[] obj)
	    {
	        return base.AddAsync(application, service, collection, obj);
	    }

        /// <summary>
        /// Updates appropriate key & corresponding value to the given reliable collection in the queryable service.
        /// SINGLE UPDATE:
        /// - PUT /query/BasicApp/ProductSvc/products and in Body provide a Json:
        /// In Body: [{
        ///	        "Key": "sku-217",
        ///	        "Value":  {
        ///	            "Sku": "sku-217",
        ///	            "Price": 11.95,
        ///	            "Quantity":40
        ///	        },
        ///	        "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
        ///	    }
        ///	    ]
        /// Record belonging to the key provided in the JSON Body of HTTP PUT Request is loacted in the partition ID mentioned & is updated with the new value, if its not existing already then its added..
        /// Incase Partition ID is not mentioned, Records with matching key from all partitions are located & updated with the new value.
        /// 
        /// 
        /// BATCH UPDATE (Provide an Array of keys & values with optional partitionID in JSON format inside body of HTTP PUT request to update all the keys with new values.). 
        /// -PUT /query/BasicApp/ProductSvc/products
        /// In Body: [{
        ///	        "Key": "sku-217",
        ///	        "Value":  {
        ///	            "Sku": "sku-217",
        ///	            "Price": 11.95,
        ///	            "Quantity":40
        ///	        },
        ///	        "PartitionId": "946fd004-37aa-4ea6-94a0-3013d8956fef"
        ///	    },
        ///     {
        ///	        "Key": "sku-218",
        ///	        "Value":  {
        ///	            "Sku": "sku-218",
        ///	            "Price": 11.85,
        ///	            "Quantity":41
        ///	        },
        ///	        "PartitionId": "a76fd004-37aa-4ea6-94a0-3013d8956fef"
        ///	    }]
        /// 
        /// Update a Key with New Value in all Partitions:
        /// -PUT /query/BasicApp/ProductSvc/products 
        /// In Body : 
        ///     [{
        ///         "Key": "sku-218"
        ///       },
        ///         "Value":  {
        ///	            "Sku": "sku-218",
        ///	            "Price": 11.85,
        ///	            "Quantity":41
        ///	    }]
        /// 
        /// </summary>
        [HttpPut]
	    [Route("query/{application}/{service}/{collection}")]
	    public Task<IHttpActionResult> UpdateAsync(string application, string service, string collection, [FromBody] ValueViewModel[] obj)
	    {
	        return base.UpdateAsync(application, service, collection, obj);
	    }
    }
}
