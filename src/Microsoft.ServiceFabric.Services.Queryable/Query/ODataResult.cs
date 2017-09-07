using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public sealed class ODataResult
	{
		[JsonProperty("odata.metadata")]
		public string ODataMetadata { get; set; }

		[JsonProperty("value")]
		public IEnumerable<JToken> Value { get; set; }
	}
}