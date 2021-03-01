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
}