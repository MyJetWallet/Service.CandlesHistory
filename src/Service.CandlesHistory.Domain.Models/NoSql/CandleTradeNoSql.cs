using System;
using MyNoSqlServer.Abstractions;

namespace Service.CandlesHistory.Domain.Models.NoSql
{
    public class CandleTradeNoSql : MyNoSqlDbEntity
    {
        public static string TableNameMinute(string brokerId) => $"candle-history-trade-{brokerId.ToLower()}-minute";
        public static string TableNameHour(string brokerId) => $"candle-history-trade-{brokerId.ToLower()}-hour";
        public static string TableNameDay(string brokerId) => $"candle-history-trade-{brokerId.ToLower()}-day";
        public static string TableNameMonth(string brokerId) => $"candle-history-trade-{brokerId.ToLower()}-month";

        public static string GeneratePartitionKey(string symbol, DateTime dateTime) => $"{symbol}::{dateTime:YYYY-MM-dd}";
        public static string GenerateRowKey(DateTime dateTime) => $"{dateTime:HH:mm:ss}";

        public CandleTrade Candle { get; set; }

        public static CandleTradeNoSql Create(string symbol, CandleTrade candle)
        {
            return new CandleTradeNoSql()
            {
                PartitionKey = GeneratePartitionKey(symbol, candle.DateTime),
                RowKey = GenerateRowKey(candle.DateTime),
                Candle = candle
            };

        }
    }
}