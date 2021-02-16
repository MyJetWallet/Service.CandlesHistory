using System;
using System.Runtime.Serialization;

namespace Service.CandlesHistory.ServiceBus
{
    [DataContract]
    public class PriceMessage
    {
        public PriceMessage()
        {
            LiquidityProvider = "default";
            Ask = Bid = TradePrice = TradeVolume = 0.0;
        }

        [DataMember(Order = 1, IsRequired = true)]
        public string Id { get; set; }

        [DataMember(Order = 2, IsRequired = true)]
        public DateTime DateTime { get; set; }

        [DataMember(Order = 3, IsRequired = false)]
        public double Bid { get; set; }

        [DataMember(Order = 4, IsRequired = false)]
        public double Ask { get; set; }

        [DataMember(Order = 5, IsRequired = false)]
        public string LiquidityProvider { get; set; } 

        [DataMember(Order = 6, IsRequired = false)]
        public double TradePrice { get; set; }

        [DataMember(Order = 7, IsRequired = false)]
        public double TradeVolume { get; set; }
    }
}