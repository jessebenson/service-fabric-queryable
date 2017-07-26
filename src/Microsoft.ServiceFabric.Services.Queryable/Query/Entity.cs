using System;

namespace Microsoft.ServiceFabric.Services.Queryable
{
	public sealed class Entity<TKey, TValue>
	{
		public Guid PartitionId { get; set; }

		public TKey Key { get; set; }

		public TValue Value { get; set; }
	}
}