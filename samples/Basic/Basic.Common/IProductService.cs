using Microsoft.ServiceFabric.Services.Remoting;
using System.Threading.Tasks;

namespace Basic.Common
{
	public interface IProductService : IService
	{
		Task<Product> GetProductAsync(string sku);

		Task UpdateProductAsync(Product product);

		Task<Product> DeleteProductAsync(string sku);
	}
}