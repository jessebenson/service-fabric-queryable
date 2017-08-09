using Newtonsoft.Json.Linq;
using System;

namespace Microsoft.ServiceFabric.Services.Queryable.Controller
{
	public enum Operation { Add, Update, Delete };

	public class ValueViewModel
	{
		public string Operation { get; set; }
		public string Collection { get; set; }
		public Guid PartitionId { get; set; }
		public JToken Key { get; set; }
		public JToken Value { get; set; }
	}
}