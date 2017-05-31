using Basic.Common;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using System;
using System.Threading.Tasks;
using System.Web.Http;

namespace Basic.WebSvc.Controllers
{
	public class ProductsController : ApiController
	{
		private static readonly Uri ProductServiceUri = new Uri("fabric:/BasicApp/ProductSvc");

		[HttpGet] // GET products/{sku}
		[Route("products/{sku}")]
		public async Task<IHttpActionResult> GetProduct([FromUri] string sku)
		{
			if (string.IsNullOrEmpty(sku))
				return BadRequest();

			var proxy = GetProductService(sku);
			var result = await proxy.GetProductAsync(sku);
			return Ok(result);
		}

		[HttpPut] // PUT products
		[Route("products")]
		public async Task<IHttpActionResult> UpdateProduct([FromBody] Product product)
		{
			if (product == null)
				return BadRequest();

			var proxy = GetProductService(product.Sku);
			await proxy.UpdateProductAsync(product);
			return Ok();
		}

		[HttpPut] // DELETE products/{sku}
		[Route("products/{sku}")]
		public async Task<IHttpActionResult> DeleteProduct([FromUri] string sku)
		{
			if (string.IsNullOrEmpty(sku))
				return BadRequest();

			var proxy = GetProductService(sku);
			var result = await proxy.DeleteProductAsync(sku);
			return Ok(result);
		}

		private IProductService GetProductService(string sku)
		{
			return ServiceProxy.Create<IProductService>(ProductServiceUri, new ServicePartitionKey(sku.GetHashCode()));
		}
	}
}
