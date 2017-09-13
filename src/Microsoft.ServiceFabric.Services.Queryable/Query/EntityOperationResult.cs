using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal sealed class EntityOperationResult
	{
		[JsonProperty("Collection")]
		public string Collection { get; set; }

		[JsonProperty("PartitionId")]
		public Guid PartitionId { get; set; }

		[JsonProperty("Key")]
		public JToken Key { get; set; }

		[JsonProperty("Status")]
		public int Status { get; set; }
	}
}