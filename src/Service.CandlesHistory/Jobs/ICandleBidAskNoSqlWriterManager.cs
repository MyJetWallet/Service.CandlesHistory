using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public interface ICandleBidAskNoSqlWriterManager
    {
        IMyNoSqlServerDataWriter<CandleBidAskNoSql> GetWriter(string brokerId, CandleType type);
    }
}