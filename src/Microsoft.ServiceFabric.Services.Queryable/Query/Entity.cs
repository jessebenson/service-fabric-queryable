using System;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public class Entity<TKey, TValue>
	{
		public Guid PartitionId { get; set; }

		public TKey Key { get; set; }

		public TValue Value { get; set; }

		public string Etag { get; set; }
	}

	public enum Operation
	{
		Add,
		Update,
		Delete,
	}

	public sealed class EntityOperation<TKey, TValue> : Entity<TKey, TValue>
	{
		public Operation Operation { get; set; }
		public string Collection { get; set; }
	}
}