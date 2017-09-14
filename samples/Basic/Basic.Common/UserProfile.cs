using System;

namespace Basic.Common
{
	public sealed class UserName : IComparable<UserName>, IEquatable<UserName>
	{
		public string First { get; set; }
		public string Last { get; set; }

		public int CompareTo(UserName other)
		{
			int comparison = string.Compare(First, other.First);
			if (comparison != 0)
				return comparison;

			return string.Compare(Last, other.Last);
		}

		public bool Equals(UserName other)
		{
			return this.CompareTo(other) == 0;
		}

		public override int GetHashCode()
		{
			int h0 = First?.GetHashCode() ?? 0;
			int h1 = Last?.GetHashCode() ?? 0;
			return h0 ^ h1;
		}
	}

	public sealed class UserProfile
	{
		public UserName Name { get; set; }
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
