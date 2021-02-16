using System;

namespace Service.CandlesHistory.Domain.Models
{
    public class CandleBidAsk
    {
        public DateTime DateTime { get; set; }

        public CandleValues Ask { get; set; }

        public CandleValues Bid { get; set; }

        public static CandleBidAsk Create(DateTime time)
        {
            return new CandleBidAsk()
            {
                DateTime = time,
                Ask = new CandleValues(),
                Bid = new CandleValues()
            };
        }
    }

    public class CandleTrade
    {
        public DateTime DateTime { get; set; }

        public CandleValues Trade { get; set; }

        public double Volume { get; set; }

        public static CandleTrade Create(DateTime time)
        {
            return new CandleTrade()
            {
                DateTime = time,
                Trade = new CandleValues(),
                Volume = 0
            };
        }
    }
}