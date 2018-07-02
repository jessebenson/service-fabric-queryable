﻿using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Mocks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.ServiceFabric.Services.Queryable;
using System.Runtime.Serialization;
using Microsoft.AspNetCore.Http;
using Moq;
using Microsoft.ServiceFabric.Data.Collections;
using Basic.Common;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Microsoft.ServiceFabric.Services.Queryable.Test
{
    [TestClass]
    public class QueryPartitionTests
    {
        IReliableStateManager userDictionaryManager;
        
        private static readonly string user0 = "\"Value\": {\r\n  \"Name\": {\r\n    \"First\": \"First0\",\r\n    \"Last\": \"Last0\"\r\n  },\r\n  \"Email\": \"user-0@example.com\",\r\n  \"Age\": 20,\r\n  \"Address\": {\r\n    \"AddressLine1\": \"10 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}";
        private static readonly string user1 = "\"Value\": {\r\n  \"Name\": {\r\n    \"First\": \"First1\",\r\n    \"Last\": \"Last1\"\r\n  },\r\n  \"Email\": \"user-1@example.com\",\r\n  \"Age\": 20,\r\n  \"Address\": {\r\n    \"AddressLine1\": \"11 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}";
        private static readonly string user2 = "\"Value\": {\r\n  \"Name\": {\r\n    \"First\": \"First2\",\r\n    \"Last\": \"Last2\"\r\n  },\r\n  \"Email\": \"user-2@example.com\",\r\n  \"Age\": 20,\r\n  \"Address\": {\r\n    \"AddressLine1\": \"12 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}";
        private static readonly string user3 = "\"Value\": {\r\n  \"Name\": {\r\n    \"First\": \"First3\",\r\n    \"Last\": \"Last3\"\r\n  },\r\n  \"Email\": \"user-3@example.com\",\r\n  \"Age\": 21,\r\n  \"Address\": {\r\n    \"AddressLine1\": \"13 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}";
        private static readonly string user4 = "\"Value\": {\r\n  \"Name\": {\r\n    \"First\": \"First4\",\r\n    \"Last\": \"Last4\"\r\n  },\r\n  \"Email\": \"user-4@example.com\",\r\n  \"Age\": 21,\r\n  \"Address\": {\r\n    \"AddressLine1\": \"14 Main St.\",\r\n    \"AddressLine2\": null,\r\n    \"City\": \"Seattle\",\r\n    \"State\": \"WA\",\r\n    \"Zipcode\": 98117\r\n  }\r\n}";

        [TestInitialize]
        public void TestInitialize()
        {
            userDictionaryManager = new MockReliableStateManager();
            IReliableDictionary2<UserName, Basic.Common.UserProfile> dictionary =
                userDictionaryManager.GetOrAddAsync<IReliableDictionary2<UserName, Basic.Common.UserProfile>>("users").Result; 

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


                    dictionary.SetAsync(tx, user.Name, user, TimeSpan.FromSeconds(4), new CancellationToken());
                    tx.CommitAsync();
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "Collection does not exist in provided statemanager")]
        public async Task CollectionDoesNotExist_ThrowsArgumentError()
        {
            IReliableStateManager stateManager = new MockReliableStateManager();
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            string collection = "collection";
            IEnumerable<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();

            var result = await stateManager.QueryPartitionAsync(httpContext.Object, collection, query, Guid.NewGuid(), new CancellationToken());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException), "Collection is not an IReliableDictionary")]
        public async Task CollectionNotADictionary_ThrowsArgumentError()
        {
            var queue = new Mock<IReliableQueue<string>>();

       
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            string collection = "collection";
            IEnumerable<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();

            var stateManager = new Mock<IReliableStateManager>();
            stateManager.Setup(obj => obj.TryGetAsync<IReliableState>(collection)).Returns(Task.FromResult(new ConditionalValue<IReliableState>(true, queue.Object)));


            var result = await ReliableStateExtensions.QueryPartitionAsync(stateManager.Object, httpContext.Object, collection, query, Guid.NewGuid(), new CancellationToken());
        }

        private HashSet<string> getProfilesFromJTokens(IEnumerable<JToken> result)
        {
            HashSet<string> profiles = new HashSet<string>();
            foreach (JToken token in result)
            {
                profiles.Add(token.First.Next.Next.ToString());
            }
            return profiles;
        }

        [TestMethod]
        public async Task EmptyQuery_NotIndexed_ReturnsUserProfile1()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            IEnumerable<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);
            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task Top1Query_NotIndexed_Returns1UserProfile()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$top", "1"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);
            int profilesreturned = 0;

            if (profiles.Contains(user0))
                profilesreturned++;
            if (profiles.Contains(user1))
                profilesreturned++;
            if (profiles.Contains(user2))
                profilesreturned++;
            if (profiles.Contains(user3))
                profilesreturned++;
            if (profiles.Contains(user4))
                profilesreturned++;

            Assert.AreEqual(1, profilesreturned);
        }

        [TestMethod]
        public async Task Top0Query_NotIndexed_Returns0UserProfiles()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$top", "0"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);
            int profilesreturned = 0;

            if (profiles.Contains(user0))
                profilesreturned++;
            if (profiles.Contains(user1))
                profilesreturned++;
            if (profiles.Contains(user2))
                profilesreturned++;
            if (profiles.Contains(user3))
                profilesreturned++;
            if (profiles.Contains(user4))
                profilesreturned++;

            Assert.AreEqual(0, profilesreturned);
        }

        [TestMethod]
        public async Task FilterEqualsOnExclusivePropertyQuery_NotIndexed_ReturnsUserProfile3()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email eq 'user-3@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterEqualsOnInlusivePropertyQuery_NotIndexed_ReturnsUserProfile3_4()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Age eq 21"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterEqualsANDQuery_NotIndexed_ReturnsUserProfile3()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Age eq 21 and Value/Email eq 'user-3@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterEqualsORQuery_NotIndexed_ReturnsUserProfile2_3_4()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Age eq 21 or Value/Email eq 'user-2@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterGreaterThanQuery_NotIndexed_ReturnsUserProfile3_4()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email gt 'user-2@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterGreaterThanOrEqualQuery_NotIndexed_ReturnsUserProfile2_3_4()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email ge 'user-2@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterLessThanOrEqualQuery_NotIndexed_ReturnsUserProfile0_1_2()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email le 'user-2@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterLessThanQuery_NotIndexed_ReturnsUserProfile0_1()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email lt 'user-2@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterLessThanAndGreaterThanQuery_NotIndexed_ReturnsUserProfile1_2_3()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email gt 'user-0@example.com' and Value/Email lt 'user-4@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterLessThanEqualAndGreaterThanEqualQuery_NotIndexed_ReturnsUserProfile1_2_3_4_5()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email ge 'user-0@example.com' and Value/Email le 'user-4@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsTrue(profiles.Contains(user0));
            Assert.IsTrue(profiles.Contains(user1));
            Assert.IsTrue(profiles.Contains(user2));
            Assert.IsTrue(profiles.Contains(user3));
            Assert.IsTrue(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterLessThanBadRangeQuery_NotIndexed_ReturnsEmpty()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Age lt 20"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilterGreaterThanBadRangeQuery_NotIndexed_ReturnsEmpty()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Age gt 30"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

        [TestMethod]
        public async Task FilternBadRange_LowGreaterThanHigh_Query_NotIndexed_ReturnsEmpty()
        {
            var httpContext = new Mock<HttpContext>();
            httpContext.Setup(obj => obj.TraceIdentifier).Returns("Test trace");
            List<KeyValuePair<string, string>> query = new List<KeyValuePair<string, string>>();
            query.Add(new KeyValuePair<string, string>("$filter", "Value/Email gt 'user-3@example.com' and Value/Email lt 'user-1@example.com'"));

            IEnumerable<JToken> result = await ReliableStateExtensions.QueryPartitionAsync(userDictionaryManager, httpContext.Object, "users", query, Guid.NewGuid(), new CancellationToken());
            var profiles = getProfilesFromJTokens(result);

            Assert.IsFalse(profiles.Contains(user0));
            Assert.IsFalse(profiles.Contains(user1));
            Assert.IsFalse(profiles.Contains(user2));
            Assert.IsFalse(profiles.Contains(user3));
            Assert.IsFalse(profiles.Contains(user4));
        }

    }

}
