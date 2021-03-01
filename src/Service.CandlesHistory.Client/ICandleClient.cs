using System;
using System.Collections.Generic;
using System.Resources;
using Service.CandlesHistory.Domain.Models;

namespace Service.CandlesHistory.Client
{
    public interface ICandleClient
    {
        IEnumerable<CandleBidAsk> GetCandlesBidAskHistoryDesc(string brokerId, string symbol, DateTime from, DateTime to, CandleType type);

        IEnumerable<CandleBidAsk> GetLastCandlesBidAskHistoryDesc(string brokerId, string symbol, int count, CandleType type);

        IEnumerable<CandleTrade> GetCandlesTradeHistoryDesc(string brokerId, string symbol, DateTime from, DateTime to, CandleType type);

        IEnumerable<CandleTrade> GetLastCandlesTradeHistoryDesc(string brokerId, string symbol, int count, CandleType type);
    }
}