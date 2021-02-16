using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public interface ICandleBidAskStore
    {
        void Save(CandleType type, string brokerId, string symbol, CandleBidAskNoSql candle);
    }
}