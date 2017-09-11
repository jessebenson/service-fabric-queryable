namespace Basic.Common
{
	public sealed class Product
	{
		public string Sku { get; set; }
		public string Name { get; set; }
		public double Price { get; set; }
	}

	public sealed class Inventory
	{
		public string Sku { get; set; }
		public int AvailableQty { get; set; }
		public int ReservedQty { get; set; }
		public int OrderedQty { get; set; }
	}
}