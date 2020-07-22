using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Couchbase.Core.IO.Operations;
using Couchbase.Core.IO.Serializers;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Core.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Transactions.Tests.UnitTests.Mocks
{
    internal class MockClusterServices : IServiceProvider
    {
        private Dictionary<Type, object> _services = new Dictionary<Type, object>()
        {
            {typeof(IRedactor), new MockRedactor()}, 
            {typeof(ITypeTranscoder), new MockTranscoder()}
        };

        public object GetService(Type serviceType)
        {
            if (_services.TryGetValue(serviceType, out var service))
            {
                return service;
            }
            else
            {
                throw new NotSupportedException($"{serviceType} has not been registered with mock DI.");
            }
        }

        internal void RegisterSingleton(Type serviceType, object singleton)
        {
            _services[serviceType] = singleton;
        }
    }

    internal class MockTranscoder : ITypeTranscoder
    {
        public Flags GetFormat<T>(T value) => new Flags() {Compression = Compression.None, DataFormat = DataFormat.Json, TypeCode = TypeCode.Object };

        public void Encode<T>(Stream stream, T value, Flags flags, OpCode opcode) => Serializer.Serialize(stream, value);

        public T Decode<T>(ReadOnlyMemory<byte> buffer, Flags flags, OpCode opcode) => Serializer.Deserialize<T>(buffer);

        public ITypeSerializer Serializer { get; set; } = new DefaultSerializer();
    }

    internal class MockRedactor : IRedactor
    {
        public object UserData(object message) => $"MOCK_USER_REDACTED({message})";

        public object MetaData(object message) => $"MOCK_META_REDACTED({message})";

        public object SystemData(object message) => $"MOCK_SYSTEM_REDACTED({message})";
    }
}
