using System;
using System.Collections.Generic;
using System.Resources;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataReader;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Client
{
    public interface ICandleClient
    {
        IEnumerable<CandleBidAsk> GetCandlesBidAskHistory(string brokerId, string symbol, DateTime from, DateTime to, CandleType type);
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

        public IEnumerable<CandleBidAsk> GetCandlesBidAskHistory(string brokerId, string symbol, DateTime from, DateTime to, CandleType type)
        {
            var reader = GetReader(brokerId, type);

            var fromDay = new DateTime(from.Year, from.Month, from.Day);
            var toDay = new DateTime(to.Year, to.Month, to.Day);

            var day = fromDay;

            while (day <= to)
            {
                var data = reader.Get(CandleBidAskNoSql.GeneratePartitionKey(day), e =>
                {
                    return true;
                    //e.Candle
                });

                //todo: добавить филььтр по символу и откинуть время в сутрках

                foreach (var item in data)
                {
                    yield return item.Candle;
                }
                
                day = day.AddDays(1);
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