using System;
using MyNoSqlServer.Abstractions;

namespace Service.CandlesHistory.Domain.Models.NoSql
{
    public class CandleBidAskNoSql: MyNoSqlDbEntity
    {
        public static string TableNameMinute(string brokerId) => $"candle-history-bidask-{brokerId.ToLower()}-minute";
        public static string TableNameHour(string brokerId) => $"candle-history-bidask-{brokerId.ToLower()}-hour";
        public static string TableNameDay(string brokerId) => $"candle-history-bidask-{brokerId.ToLower()}-day";
        public static string TableNameMonth(string brokerId) => $"candle-history-bidask-{brokerId.ToLower()}-month";

        public static string GeneratePartitionKey(DateTime dateTime) => $"{dateTime:yyyy-MM-dd}";
        public static string GenerateRowKey(string symbol, DateTime dateTime) => $"{symbol}::{dateTime:HH:mm:ss}";

        public CandleBidAsk Candle { get; set; }

        public static CandleBidAskNoSql Create(string symbol, CandleBidAsk candle)
        {
            return new CandleBidAskNoSql()
            {
                PartitionKey = GeneratePartitionKey(candle.DateTime),
                RowKey = GenerateRowKey(symbol, candle.DateTime),
                Candle = candle
            };

        }
    }
}