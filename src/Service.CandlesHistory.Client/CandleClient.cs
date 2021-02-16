using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.DataReader;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Client
{
    public class CandleClient : ICandleClient
    {
        private readonly Func<string> _getMyNoSqlHostPort;
        private readonly string _appName;
        private readonly Dictionary<string, MyNoSqlTcpClient> _myNoSqlSubscriberByBroker = new Dictionary<string, MyNoSqlTcpClient>();

        private readonly object _gate = new object();

        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _minuteReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _hourReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _dayReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _monthReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();

        public CandleClient(Func<string> getMyNoSqlHostPort, string appName)
        {
            _getMyNoSqlHostPort = getMyNoSqlHostPort;
            _appName = appName;
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

                RegisterNoSqlClient(brokerId);
                return _minuteReaderByBroker[brokerId];
            }

            if (type == CandleType.Hour)
            {
                if (_hourReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _hourReaderByBroker[brokerId];
            }

            if (type == CandleType.Day)
            {
                if (_dayReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _dayReaderByBroker[brokerId];
            }

            if (type == CandleType.Month)
            {
                if (_monthReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                
                RegisterNoSqlClient(brokerId);
                return _monthReaderByBroker[brokerId];
            }

            throw new Exception($"Unknown candle type {type}");
        }

        private void RegisterNoSqlClient(string brokerId)
        {
            MyNoSqlTcpClient client;
            lock (_gate)
            {
                if (_myNoSqlSubscriberByBroker.TryGetValue(brokerId, out client))
                    return;

                client = new MyNoSqlTcpClient(_getMyNoSqlHostPort, $"{_appName}-{brokerId}");

                _minuteReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameMinute(brokerId));
                _hourReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameHour(brokerId));
                _dayReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameDay(brokerId));
                _monthReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameMonth(brokerId));

                _myNoSqlSubscriberByBroker[brokerId] = client;
            }

            client.Start();

            //todo: use wait initialization

            var index = 0;
            while (index < 50)
            {
                if (_minuteReaderByBroker[brokerId].Count() > 0
                    && _hourReaderByBroker[brokerId].Count() > 0
                    && _dayReaderByBroker[brokerId].Count() > 0
                    && _monthReaderByBroker[brokerId].Count() > 0)
                {
                    break;
                }

                Thread.Sleep(100);
                index++;
            }
        }
    }
}