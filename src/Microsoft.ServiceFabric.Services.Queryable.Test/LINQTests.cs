using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Basic.Common;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Indexing.Persistent;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.ServiceFabric.Services.Queryable.LINQ;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
    [TestClass]
    public class LINQTests
    {
        IReliableStateManager userDictionaryManager;
        IReliableIndexedDictionary<UserName, Basic.Common.UserProfile> indexed_users;

        private static readonly Basic.Common.UserProfile user0 = JToken.Parse("{\r\n  \"Email\": \"user-0@example.com\",\r\n  \"Age\": 20,\r\n  \"Name\": {\r\n    \"First\": \"First0\",\r\n    \"Last\": \"Last0\"\r\n  },\r\n  \"Address\": {\r\n    \"AddressLine1\": \"10 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}").ToObject<Basic.Common.UserProfile>();
        private static readonly Basic.Common.UserProfile user1 = JToken.Parse("{\r\n  \"Email\": \"user-1@example.com\",\r\n  \"Age\": 20,\r\n  \"Name\": {\r\n    \"First\": \"First1\",\r\n    \"Last\": \"Last1\"\r\n  },\r\n  \"Address\": {\r\n    \"AddressLine1\": \"11 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}").ToObject<Basic.Common.UserProfile>();
        private static readonly Basic.Common.UserProfile user2 = JToken.Parse("{\r\n  \"Email\": \"user-2@example.com\",\r\n  \"Age\": 20,\r\n  \"Name\": {\r\n    \"First\": \"First2\",\r\n    \"Last\": \"Last2\"\r\n  },\r\n  \"Address\": {\r\n    \"AddressLine1\": \"12 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}").ToObject<Basic.Common.UserProfile>();
        private static readonly Basic.Common.UserProfile user3 = JToken.Parse("{\r\n  \"Email\": \"user-3@example.com\",\r\n  \"Age\": 21,\r\n  \"Name\": {\r\n    \"First\": \"First3\",\r\n    \"Last\": \"Last3\"\r\n  },\r\n  \"Address\": {\r\n    \"AddressLine1\": \"13 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}").ToObject<Basic.Common.UserProfile>();
        private static readonly Basic.Common.UserProfile user4 = JToken.Parse("{\r\n  \"Email\": \"user-4@example.com\",\r\n  \"Age\": 21,\r\n  \"Name\": {\r\n    \"First\": \"First4\",\r\n    \"Last\": \"Last4\"\r\n  },\r\n  \"Address\": {\r\n    \"AddressLine1\": \"14 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}").ToObject<Basic.Common.UserProfile>();

        [TestInitialize]
        public void TestInitialize()
        {
            userDictionaryManager = new MockReliableStateManager();

            indexed_users = userDictionaryManager.GetOrAddIndexedAsync<UserName, Basic.Common.UserProfile>("indexed_users",
                 FilterableIndex<UserName, Basic.Common.UserProfile, string>.CreateQueryableInstance("Email"),
                   FilterableIndex<UserName, Basic.Common.UserProfile, int>.CreateQueryableInstance("Age")).Result;

            for (int i = 0; i < 5; i++)
            {
                using (var tx = userDictionaryManager.CreateTransaction())
                {
                    var user = new Basic.Common.UserProfile
                    {
                        Name = new UserName
                        {
                            First = $"First{i}",
                            Last = $"Last{i}",
                        },
                        Email = $"user-{i}@example.com",
                        Age = 20 + i / 3,
                        Address = new Basic.Common.Address
                        {
                            AddressLine1 = $"1{i} Main St.",
                            City = "Seattle",
                            State = "WA",
                            Zipcode = 98117,
                        },
                    };

                    indexed_users.SetAsync(tx, user.Name, user, TimeSpan.FromSeconds(4), new CancellationToken());
                    tx.CommitAsync();
                }
            }
            Assert.IsTrue(userDictionaryManager.TryGetIndexedAsync<UserName, Basic.Common.UserProfile>("indexed_users",
                    FilterableIndex<UserName, Basic.Common.UserProfile, string>.CreateQueryableInstance("Email"),
                    FilterableIndex<UserName, Basic.Common.UserProfile, int>.CreateQueryableInstance("Age")).Result.HasValue);
        }

        [TestMethod]
        public void SelectProfile_ReturnsAllUsers()
        {
            var qdict = new QueryableReliableIndexedDictionary<UserName, Basic.Common.UserProfile, Basic.Common.UserProfile>(indexed_users, userDictionaryManager);
            var query = qdict.Select(x => x);

            ISet<Basic.Common.UserProfile> profiles = new SortedSet<Basic.Common.UserProfile>();
            // Execute the queries, add breakpoints here to see results
            foreach (Basic.Common.UserProfile profile in query)
            {
                profiles.Add(profile);
            }

            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public void SelectProfileEmails_ReturnsAllUsersEmails()
        {
            var qdict = new QueryableReliableIndexedDictionary<UserName, Basic.Common.UserProfile, Basic.Common.UserProfile>(indexed_users, userDictionaryManager);

            var query = qdict.Select(x => x.Email);

            ISet<string> emails = new SortedSet<string>();

            // Execute the queries, add breakpoints here to see results
            foreach (var email in query)
            {
                emails.Add(email);
            }

            Assert.IsTrue(emails.Contains("user-0@example.com"));
            Assert.IsTrue(emails.Contains("user-1@example.com"));
            Assert.IsTrue(emails.Contains("user-2@example.com"));
            Assert.IsTrue(emails.Contains("user-3@example.com"));
            Assert.IsTrue(emails.Contains("user-4@example.com"));
        }


        [TestMethod]
        public void WhereProfileEmailEquals_ReturnsUser1()
        {
            var qdict = new QueryableReliableIndexedDictionary<UserName, Basic.Common.UserProfile, Basic.Common.UserProfile>(indexed_users, userDictionaryManager);

            var query = from Basic.Common.UserProfile profile in qdict
                        where profile.Email == "user-1@example.com"
                        select profile;

            ISet < Basic.Common.UserProfile > profiles = new SortedSet<Basic.Common.UserProfile>();
            // Execute the queries, add breakpoints here to see results
            foreach (Basic.Common.UserProfile profile in query)
            {
                profiles.Add(profile);
            }

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public void WhereProfileEmailGreaterThan_ReverseOrder_ReturnsUser3_4()
        {
            var qdict = new QueryableReliableIndexedDictionary<UserName, Basic.Common.UserProfile, Basic.Common.UserProfile>(indexed_users, userDictionaryManager);

            var query = qdict.Where(x => "user-2@example.com".CompareTo(x.Email) > 0);

            ISet<Basic.Common.UserProfile> profiles = new SortedSet<Basic.Common.UserProfile>();
            // Execute the queries, add breakpoints here to see results
            foreach (Basic.Common.UserProfile profile in query)
            {
                profiles.Add(profile);
            }

            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public void WhereProfileEmailGreaterThan_ReturnsUser3_4()
        {
            var qdict = new QueryableReliableIndexedDictionary<UserName, Basic.Common.UserProfile, Basic.Common.UserProfile>(indexed_users, userDictionaryManager);

            var query = qdict.Where(x => x.Email.CompareTo("user-2@example.com") >= 0);

            ISet<Basic.Common.UserProfile> profiles = new SortedSet<Basic.Common.UserProfile>();
            // Execute the queries, add breakpoints here to see results
            foreach (Basic.Common.UserProfile profile in query)
            {
                profiles.Add(profile);
            }

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

    }
}
