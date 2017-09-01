namespace Microsoft.ServiceFabric.Services.Queryable.Controller
{
	public class BackendViewModel
	{
		public Operation Operation { get; set; }
		public string Collection { get; set; }
		public string Key { get; set; }
		public string Value { get; set; }
		public string Etag { get; set; }
	}
}