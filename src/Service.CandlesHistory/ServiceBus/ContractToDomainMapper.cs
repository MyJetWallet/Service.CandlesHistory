using System;
using System.IO;
using JetBrains.Annotations;

namespace Service.CandlesHistory.ServiceBus
{
    public static class ContractToDomainMapper
    {
        public static bool IsDebug { get; set; } = false;

        [UsedImplicitly]
        public static T ByteArrayToServiceBusContract<T>(this ReadOnlyMemory<byte> data)
        {
            if (IsDebug)
                Console.WriteLine($"GOT {typeof(T)} MESSAGE LEN:" + data.Length);

            var span = data.Span;

            if (span[0] == 0)
            {
                span = data.Slice(1, data.Length - 1).Span;
                var mem = new MemoryStream(data.Length);
                mem.Write(span);
                mem.Position = 0;
                var res = ProtoBuf.Serializer.Deserialize<T>(mem);
                return res;
            }

            throw new Exception("Not supported version of Contract");
        }

    }
}