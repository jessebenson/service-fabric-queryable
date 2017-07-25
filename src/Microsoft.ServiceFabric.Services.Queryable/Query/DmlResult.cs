using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;


namespace Microsoft.ServiceFabric.Services.Queryable
{
	public class DmlResult
	{
		[JsonProperty("PartitionId")]
		public Guid PartitionId { get; set; }

		[JsonProperty("Key")]
		public JToken Key { get; set; }

		[JsonProperty("Status")]
		public int Status { get; set; }
	}
}