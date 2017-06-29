using Microsoft.ServiceFabric.Services.Remoting;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	/// <summary>
	/// This is the service interface that must be implemented by stateful services that support querying.
	/// </summary>
	public interface IQueryableService : IService
	{
		/// <summary>
		/// Query the service for the OData $metadata specification.
		/// </summary>
		/// <returns>Metadata about the current reliable collections and their types.</returns>
		Task<string> GetMetadataAsync();

		/// <summary>
		/// Query the service using the query parameters defined in <paramref name="query"/> against
		/// the reliable collection with the name <paramref name="collection"/>.
		/// 
		/// The service partition receiving this call is responsible for querying all other partitions
		/// and aggregating the results.
		/// </summary>
		/// <param name="collection">The reliable collection to query.</param>
		/// <param name="query">OData query options.</param>
		/// <returns>The json serialized results from the query.</returns>
		Task<IEnumerable<string>> QueryAsync(string collection, IEnumerable<KeyValuePair<string, string>> query);

		/// <summary>
		/// Query the service partition using the query parameters defined in <paramref name="query"/> against
		/// the reliable collection with the name <paramref name="collection"/>.
		/// </summary>
		/// <param name="collection">The reliable collection to query.</param>
		/// <param name="query">OData query options.</param>
		/// <returns>The json serialized results from the query.</returns>
		Task<IEnumerable<string>> QueryPartitionAsync(string collection, IEnumerable<KeyValuePair<string, string>> query);

        Task<bool> DeleteAsync(string collection, string key);

    }
}
