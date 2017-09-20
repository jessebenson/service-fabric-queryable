using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal static class StatefulServiceUtils
	{
		private static readonly FabricClient FabricClient = new FabricClient();
		private static readonly HttpClient HttpClient = new HttpClient();

		public static async Task<string> QueryPartitionAsync(string endpoint, Guid partitionId, string collection, IEnumerable<KeyValuePair<string, string>> query)
		{
			string requestUri = $"{endpoint}/query/{partitionId}/{collection}?{GetQueryParameters(query)}";
			var response = await HttpClient.GetAsync(requestUri).ConfigureAwait(false);
			return await response.EnsureSuccessStatusCode().Content.ReadAsStringAsync().ConfigureAwait(false);
		}

		public static async Task<IEnumerable<Partition>> GetPartitionsAsync(StatefulServiceContext serviceContext)
		{
			var partitions = await FabricClient.QueryManager.GetPartitionListAsync(serviceContext.ServiceName).ConfigureAwait(false);
			return partitions.Where(p => p.PartitionInformation.Id != serviceContext.PartitionId);
		}

		/// <summary>
		/// Resolve the primary query http endpoint for the stateful service by partition id.
		/// </summary>
		public static async Task<string> GetPartitionEndpointAsync(StatefulServiceContext serviceContext, Guid partitionId)
		{
			var partitions = await FabricClient.QueryManager.GetPartitionListAsync(serviceContext.ServiceName).ConfigureAwait(false);
			var partition = partitions.FirstOrDefault(p => p.PartitionInformation.Id == partitionId);
			if (partition == null)
				throw new QueryException(HttpStatusCode.NotFound, $"Primary endpoint for partition '{partitionId}' not found.");

			return await GetPartitionEndpointAsync(serviceContext, partition).ConfigureAwait(false);
		}

		/// <summary>
		/// Resolve the primary query http endpoint for the stateful service by partition.
		/// </summary>
		public static async Task<string> GetPartitionEndpointAsync(StatefulServiceContext serviceContext, Partition partition)
		{
			// Resolve the endpoint for the stateful service.
			string endpoints = null;
			if (partition.PartitionInformation.Kind == ServicePartitionKind.Int64Range)
				endpoints = await GetPartitionEndpointAsync(serviceContext, (Int64RangePartitionInformation)partition.PartitionInformation).ConfigureAwait(false);
			else if (partition.PartitionInformation.Kind == ServicePartitionKind.Named)
				endpoints = await GetPartitionEndpointAsync(serviceContext, (NamedPartitionInformation)partition.PartitionInformation).ConfigureAwait(false);
			else if (partition.PartitionInformation.Kind == ServicePartitionKind.Singleton)
				endpoints = await GetPartitionEndpointAsync(serviceContext, (SingletonPartitionInformation)partition.PartitionInformation).ConfigureAwait(false);

			// Try to find the query http endpoint.
			if (string.IsNullOrEmpty(endpoints))
				throw new QueryException(HttpStatusCode.NotFound, $"Primary endpoint for partition '{partition.PartitionInformation.Id}' not found.");
			var endpointMap = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, string>>>(endpoints);
			if (!endpointMap.TryGetValue("Endpoints", out Dictionary<string, string> namedEndpoints))
				throw new QueryException(HttpStatusCode.NotFound, $"Primary endpoint for partition '{partition.PartitionInformation.Id}' not found.");
			if (!TryGetQueryEndpoint(namedEndpoints, out string endpoint))
				throw new QueryException(HttpStatusCode.NotFound, $"Primary endpoint for partition '{partition.PartitionInformation.Id}' not found.");

			return endpoint;
		}

		private static async Task<string> GetPartitionEndpointAsync(StatefulServiceContext serviceContext, Int64RangePartitionInformation partition)
		{
			var resolvedPartition = await FabricClient.ServiceManager.ResolveServicePartitionAsync(serviceContext.ServiceName, partition.LowKey).ConfigureAwait(false);
			return resolvedPartition?.GetEndpoint()?.Address;
		}

		private static async Task<string> GetPartitionEndpointAsync(StatefulServiceContext serviceContext, NamedPartitionInformation partition)
		{
			var resolvedPartition = await FabricClient.ServiceManager.ResolveServicePartitionAsync(serviceContext.ServiceName, partition.Name).ConfigureAwait(false);
			return resolvedPartition?.GetEndpoint()?.Address;
		}

		private static async Task<string> GetPartitionEndpointAsync(StatefulServiceContext serviceContext, SingletonPartitionInformation partition)
		{
			var resolvedPartition = await FabricClient.ServiceManager.ResolveServicePartitionAsync(serviceContext.ServiceName).ConfigureAwait(false);
			return resolvedPartition?.GetEndpoint()?.Address;
		}

		private static string GetQueryParameters(IEnumerable<KeyValuePair<string, string>> query)
		{
			return string.Join("&", query.Select(p => $"{p.Key}={p.Value}"));
		}

		/// <summary>
		/// Try to find the query endpoint from the set of stateful service endpoints.
		/// </summary>
		private static bool TryGetQueryEndpoint(Dictionary<string, string> endpoints, out string endpoint)
		{
			endpoint = null;

			// Try the default endpoint first.
			if (endpoints.TryGetValue("", out endpoint) && endpoint.StartsWith("http"))
				return true;

			// Check for any http endpoint.
			foreach (var endpointPair in endpoints)
			{
				if (endpointPair.Value.StartsWith("http"))
				{
					endpoint = endpointPair.Value;
					return true;
				}
			}

			return false;
		}
	}
}
