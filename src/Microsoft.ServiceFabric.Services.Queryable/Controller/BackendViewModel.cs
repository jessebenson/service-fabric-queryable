using Newtonsoft.Json.Linq;
using System;

namespace Microsoft.ServiceFabric.Services.Queryable.Controller
{

	public class BackendViewModel
	{
		public string Operation { get; set; }
		public string Collection { get; set; }
		
		public string Key { get; set; }
		public string Value { get; set; }
	}
}