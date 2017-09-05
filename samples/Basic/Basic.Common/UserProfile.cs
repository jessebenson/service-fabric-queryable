namespace Basic.Common
{
	public sealed class UserProfile
	{
		public string Name { get; set; }
		public string Email { get; set; }
		public int Age { get; set; }
		public Address Address { get; set; }
	}

	public sealed class Address
	{
		public string AddressLine1 { get; set; }
		public string AddressLine2 { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public int Zipcode { get; set; }
	}
}
