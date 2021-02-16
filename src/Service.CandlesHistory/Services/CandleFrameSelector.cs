using System;
using Service.CandlesHistory.Domain.Models;

namespace Service.CandlesHistory.Services
{
    public class CandleFrameSelector
    {
        public DateTime SelectFrame(DateTime dt, CandleType candle)
        {
            switch (candle)
            {
                case CandleType.Minute:
                    return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0);

                case CandleType.Hour:
                    return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0);

                case CandleType.Day:
                    return new DateTime(dt.Year, dt.Month, dt.Day, 0, 0, 0);

                case CandleType.Month:
                    return new DateTime(dt.Year, dt.Month, 1, 0, 0, 0);
            }

            throw new Exception($"Unknown candle type {candle}");
        }
    }
}