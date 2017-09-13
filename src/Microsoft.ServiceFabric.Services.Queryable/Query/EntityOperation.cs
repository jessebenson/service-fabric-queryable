namespace Microsoft.ServiceFabric.Services.Queryable
{
	internal sealed class EntityOperation<TKey, TValue> : Entity<TKey, TValue>
	{
		public Operation Operation { get; set; }
		public string Collection { get; set; }
	}
}