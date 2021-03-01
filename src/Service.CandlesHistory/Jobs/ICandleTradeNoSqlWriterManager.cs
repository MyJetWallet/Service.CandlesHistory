using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public interface ICandleTradeNoSqlWriterManager
    {
        IMyNoSqlServerDataWriter<CandleTradeNoSql> GetWriter(string brokerId, CandleType type);
    }
}