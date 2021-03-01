using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public interface ICandleTradeStoreJob
    {
        void Save(CandleType type, string brokerId, string symbol, CandleTradeNoSql candle);
    }
}