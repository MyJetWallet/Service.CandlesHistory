using System;
using System.Collections.Generic;
using System.Linq;
using System.Resources;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataReader;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Client
{
    public interface ICandleClient
    {
        IEnumerable<CandleBidAsk> GetCandlesBidAskHistoryDesc(string brokerId, string symbol, DateTime from, DateTime to, CandleType type);

        IEnumerable<CandleBidAsk> GetLastCandlesBidAskHistoryDesc(string brokerId, string symbol, int count, CandleType type);
    }

    public class CandleClient : ICandleClient
    {
        private readonly MyNoSqlSubscriber _myNoSqlSubscriber;

        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _minuteReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _hourReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _dayReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _monthReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();

        public CandleClient(MyNoSqlSubscriber myNoSqlSubscriber)
        {
            _myNoSqlSubscriber = myNoSqlSubscriber;
        }

        public IEnumerable<CandleBidAsk> GetCandlesBidAskHistoryDesc(string brokerId, string symbol, DateTime from, DateTime to, CandleType type)
        {
            var reader = GetReader(brokerId, type);

            var day = new DateTime(to.Year, to.Month, to.Day);

            var end = from.AddDays(-1);
            while (day >= end)
            {
                var data = reader.Get(CandleBidAskNoSql.GeneratePartitionKey(symbol, day), 
                    entity => entity.Candle.DateTime >= from && entity.Candle.DateTime <= to);

                if (data != null)
                {
                    foreach (var item in data.Select(e => e.Candle).OrderByDescending(e => e.DateTime))
                    {
                        yield return item;
                    }
                }

                day = day.AddDays(-1);
            }
        }

        public IEnumerable<CandleBidAsk> GetLastCandlesBidAskHistoryDesc(string brokerId, string symbol, int count, CandleType type)
        {
            var now = DateTime.UtcNow;
            var day = new DateTime(now.Year, now.Month, now.Day);

            var to = now.AddYears(-10);

            var index = 0;

            var reader = GetReader(brokerId, type);

            while (index < count && day > to)
            {
                var data = reader.Get(CandleBidAskNoSql.GeneratePartitionKey(symbol, day));

                if (data != null)
                {
                    foreach (var item in data.Select(e => e.Candle).OrderByDescending(e => e.DateTime))
                    {
                        index++;
                        yield return item;
                        
                        if (index >= count)
                            break;
                    }
                }

                day = day.AddDays(-1);
            }
        }




        private IMyNoSqlServerDataReader<CandleBidAskNoSql> GetReader(string brokerId, CandleType type)
        {
            if (type == CandleType.Minute)
            {
                if (_minuteReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                reader = new MyNoSqlReadRepository<CandleBidAskNoSql>(_myNoSqlSubscriber, CandleBidAskNoSql.TableNameMinute(brokerId));
                _minuteReaderByBroker[brokerId] = reader;
                return reader;
            }

            if (type == CandleType.Hour)
            {
                if (_hourReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                reader = new MyNoSqlReadRepository<CandleBidAskNoSql>(_myNoSqlSubscriber, CandleBidAskNoSql.TableNameHour(brokerId));
                _hourReaderByBroker[brokerId] = reader;
                return reader;
            }

            if (type == CandleType.Day)
            {
                if (_dayReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                reader = new MyNoSqlReadRepository<CandleBidAskNoSql>(_myNoSqlSubscriber, CandleBidAskNoSql.TableNameDay(brokerId));
                _dayReaderByBroker[brokerId] = reader;
                return reader;
            }

            if (type == CandleType.Month)
            {
                if (_monthReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                reader = new MyNoSqlReadRepository<CandleBidAskNoSql>(_myNoSqlSubscriber, CandleBidAskNoSql.TableNameMonth(brokerId));
                _monthReaderByBroker[brokerId] = reader;
                return reader;
            }

            throw new Exception($"Unknown candle type {type}");
        }
    }
}