////using System;
////using System.Collections.Generic;
////using System.Text;
////using System.Threading.Tasks;
////using Couchbase.KeyValue;
////using Xunit;

////namespace Couchbase.Transactions.Tests.UnitTests.Mocks
////{
////    /// <summary>
////    /// A class that tests if the mocks themselves are working.
////    /// </summary>
////    public class MockTests
////    {
////        [Fact]
////        public async Task MockCollection_LookupIn()
////        {
////            var content = new
////            {
////                foo = "bar",
////                baz = new
////                {
////                    omg = "norly"
////                }
////            };

////            var mockDocs = new List<ObjectGetResult>()
////            {
////                new ObjectGetResult("id1", content, new Dictionary<string, object>()
////                {
////                    { "$document.CAS", 1234 },
////                    { "$document.exptime", 11010103993 },
////                    { "txn.id.txn", "6a919480-d9fd-4d0d-9074-2506b3a210d1" }
////                })
////            };

////            var collection = new MockCollection(mockDocs);
////            var lookupInResults = await collection.LookupInAsync("id1", spec =>
////                spec.Get("$document.exptime", true)
////                    .Get("txn.id.txn", true)
////                    .Get("$document.exptime_not", true)
////                    .Get("xattr.does.not.exist", true)
////                    .Get("doc.path.does.not.exist")
////                    .Get("baz.omg")
////                    .GetFull());

////            foreach (var i in new[] {0, 1, 5, 6})
////            {
////                Assert.True(lookupInResults.Exists(i));
////            }

////            foreach (var i in new[] {2, 3, 4})
////            {
////                Assert.False(lookupInResults.Exists(i));
////            }

////            Assert.Equal("norly", lookupInResults.ContentAs<string>(5));
////        }
////    }
////}
