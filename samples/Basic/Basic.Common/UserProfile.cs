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

        public static int Compare(UserName name, UserName name2)
        {
            if (name == null && name2 == null)
            {
                return 0;
            }
            else if (name == null)
            {
                return -1;
            }
            else if (name2 == null)
            {
                return 1;
            }
            return name.CompareTo(name2);
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

	public sealed class UserProfile : IComparable<UserProfile>
	{
		public UserName Name { get; set; }
		public string Email { get; set; }
		public int Age { get; set; }
		public Address Address { get; set; }

        public int CompareTo(UserProfile other)
        {
            // Email Priority (most likely to be different)
            int comparison = string.Compare(Email, other.Email);
            if (comparison != 0)
                return comparison;

            // then Name
            comparison = UserName.Compare(Name, other.Name);
            if (comparison != 0)
                return comparison;

            // then Address
            comparison = Address.CompareTo(other.Address);
            if (comparison != 0)
                return comparison;

            // then Age
            return Age.CompareTo(other.Age);
        }

        public bool Equals(UserProfile other)
        {
            return this.CompareTo(other) == 0;
        }

        public override int GetHashCode()
        {
            int h0 = Email?.GetHashCode() ?? 0;
            int h1 = Name?.GetHashCode() ?? 0;
            int h2 = Address?.GetHashCode() ?? 0;
            return h0 ^ h1 ^ h2 ^ Age;
        }
    }

	public sealed class Address
	{
		public string AddressLine1 { get; set; }
		public string AddressLine2 { get; set; }
		public string City { get; set; }
		public string State { get; set; }
		public int Zipcode { get; set; }

        static int Compare (Address ad1, Address ad2)
        {
            if (ad1 == null && ad2 == null)
            {
                return 0;
            }
            else if (ad1 == null)
            {
                return -1;
            }
            else if (ad2 == null)
            {
                return 1;
            }
            return ad1.CompareTo(ad2);
        }

        internal int CompareTo(Address other)
        {
            // First Line Priority (most likely to be different)
            int comparison = string.Compare(AddressLine1, other.AddressLine1);
            if (comparison != 0)
                return comparison;

            // then Line2
            comparison = string.Compare(AddressLine2, other.AddressLine2);
            if (comparison != 0)
                return comparison;

            // then ZipCode
            comparison = Zipcode.CompareTo(other.Zipcode);
            if (comparison != 0)
                return comparison;

            // then City
            comparison = string.Compare(City, other.City);
            if (comparison != 0)
                return comparison;

            //finally state
            return string.Compare(State, other.State);
        }

        public override int GetHashCode()
        {
            int h0 = AddressLine1?.GetHashCode() ?? 0;
            int h1 = AddressLine2?.GetHashCode() ?? 0;
            int h2 = City?.GetHashCode() ?? 0;
            int h3 = State?.GetHashCode() ?? 0;
            return h0 ^ h1 ^ h2 ^ h3 ^ Zipcode;
        }
    }
}
