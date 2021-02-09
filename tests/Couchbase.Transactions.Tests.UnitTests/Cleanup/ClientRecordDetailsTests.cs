using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Couchbase.Transactions.ActiveTransactionRecords;
using Couchbase.Transactions.Cleanup.LostTransactions;
using Couchbase.Transactions.Components;
using Couchbase.Transactions.DataModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Couchbase.Transactions.Tests.UnitTests.Cleanup
{
    public class ClientRecordDetailsTests
    {
        private readonly ITestOutputHelper _outputHelper;

        public ClientRecordDetailsTests(ITestOutputHelper _outputHelper)
        {
            this._outputHelper = _outputHelper;
        }

        [Fact]
        public void This_Client_Does_Not_Count_As_Expired()
        {
            var clientUuid = Guid.NewGuid().ToString();
            var clientRecord = new ClientRecordsIndex()
            {
                Clients = new Dictionary<string, ClientRecordEntry>()
                {
                    // Other, expired client.
                    { Guid.NewGuid().ToString(),
                        new ClientRecordEntry()
                        {
                            ExpiresMilliseconds = 10,
                            HeartbeatMutationCas = "0x0000b12a92016516",
                            NumAtrs = 1024
                        }
                    },

                    // This client, also expired.
                    { clientUuid,
                        new ClientRecordEntry()
                        {
                            ExpiresMilliseconds = 10,
                            HeartbeatMutationCas = "0x0000b12a92016516",
                            NumAtrs = 1024
                        }
                    }
                }
            };

            var parsedHlc = new ParsedHLC(DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture), "real");

            var clientRecordDetails = new ClientRecordDetails(clientRecord, parsedHlc, clientUuid, TimeSpan.FromSeconds(1));
            _outputHelper.WriteLine(JObject.FromObject(clientRecordDetails).ToString());
            Assert.Equal(2, clientRecordDetails.NumExistingClients);
            Assert.Equal(1, clientRecordDetails.NumExpiredClients);
            Assert.Equal(1, clientRecordDetails.ExpiredClientIds.Count);
            Assert.Contains(clientUuid, clientRecordDetails.SortedActiveClientIds);
            Assert.NotEqual(-1, clientRecordDetails.IndexOfThisClient);
        }

        [Fact]
        public void This_Client_Always_Contains_Active_Current_ClientId()
        {
            var clientUuid = Guid.NewGuid().ToString();
            var clientRecord = new ClientRecordsIndex()
            {
                Clients = new Dictionary<string, ClientRecordEntry>()
            };

            var parsedHlc = new ParsedHLC("0", "real");

            var clientRecordDetails = new ClientRecordDetails(clientRecord, parsedHlc, clientUuid, TimeSpan.FromSeconds(1));
            Assert.Equal(0, clientRecordDetails.NumExpiredClients);
            Assert.Equal(0, clientRecordDetails.ExpiredClientIds.Count);
            Assert.Contains(clientUuid, clientRecordDetails.SortedActiveClientIds);
            Assert.Equal(0, clientRecordDetails.IndexOfThisClient);
        }
    }
}
