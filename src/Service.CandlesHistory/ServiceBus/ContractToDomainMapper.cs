using System;
using System.IO;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace Service.CandlesHistory.ServiceBus
{
    public static class ContractToDomainMapper
    {
        public static bool IsDebug { get; set; } = false;

        [UsedImplicitly]
        public static PriceMessage ByteArrayToServiceBusContract(this ReadOnlyMemory<byte> data)
        {
            if (IsDebug)
                Console.WriteLine($"GOT {typeof(PriceMessage)} MESSAGE LEN:" + data.Length);

            var span = data.Span;

            try
            {
                if (span[0] == 0)
                {
                    span = data.Slice(1, data.Length - 1).Span;
                    var mem = new MemoryStream(data.Length);
                    mem.Write(span);
                    mem.Position = 0;

                    try
                    {
                        var res = ProtoBuf.Serializer.Deserialize<PriceMessage>(mem);
                        return res;
                    }
                    catch (Exception ex)
                    {
                        mem.Position = 0;
                        var res = ProtoBuf.Serializer.Deserialize<PriceMessageOld>(mem);
                        return new PriceMessage()
                        {
                            Id = res.Id,
                            Ask = (double) res.Ask,
                            Bid = (double) res.Bid,
                            DateTime = res.DateTime,
                            LiquidityProvider = res.LiquidityProvider,
                            TradePrice = (double) res.TradePrice,
                            TradeVolume = (double) res.TradeVolume
                        };
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"ServiceSub deserialization exception: {ex}");
                throw;
            }

            throw new Exception("Not supported version of Contract");
        }

        [DataContract]
        public class PriceMessageOld
        {
            [DataMember(Order = 1, IsRequired = true)]
            public string Id { get; set; }

            [DataMember(Order = 2, IsRequired = true)]
            public DateTime DateTime { get; set; }

            [DataMember(Order = 3, IsRequired = false)]
            public decimal Bid { get; set; }

            [DataMember(Order = 4, IsRequired = false)]
            public decimal Ask { get; set; }

            [DataMember(Order = 5, IsRequired = false)]
            public string LiquidityProvider { get; set; }

            [DataMember(Order = 6, IsRequired = false)]
            public decimal TradePrice { get; set; }

            [DataMember(Order = 7, IsRequired = false)]
            public decimal TradeVolume { get; set; }
        }
    }
}