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

        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _minuteBidAskReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _hourBidAskReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _dayBidAskReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>> _monthBidAskReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleBidAskNoSql>>();

        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>> _minuteTradeReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>> _hourTradeReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>> _dayTradeReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>> _monthTradeReaderByBroker = new Dictionary<string, IMyNoSqlServerDataReader<CandleTradeNoSql>>();

        public CandleClient(Func<string> getMyNoSqlHostPort, string appName)
        {
            _getMyNoSqlHostPort = getMyNoSqlHostPort;
            _appName = appName;
        }

        public IEnumerable<CandleBidAsk> GetCandlesBidAskHistoryDesc(string brokerId, string symbol, DateTime from, DateTime to, CandleType type)
        {
            var reader = GetReaderBidAsk(brokerId, type);

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

            var reader = GetReaderBidAsk(brokerId, type);

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

        public IEnumerable<CandleTrade> GetCandlesTradeHistoryDesc(string brokerId, string symbol, DateTime @from, DateTime to, CandleType type)
        {
            var reader = GetReaderTrade(brokerId, type);

            var day = new DateTime(to.Year, to.Month, to.Day);

            var end = from.AddDays(-1);
            while (day >= end)
            {
                var data = reader.Get(CandleTradeNoSql.GeneratePartitionKey(symbol, day),
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

        public IEnumerable<CandleTrade> GetLastCandlesTradeHistoryDesc(string brokerId, string symbol, int count, CandleType type)
        {
            var now = DateTime.UtcNow;
            var day = new DateTime(now.Year, now.Month, now.Day);

            var to = now.AddYears(-10);

            var index = 0;

            var reader = GetReaderTrade(brokerId, type);

            while (index < count && day > to)
            {
                var data = reader.Get(CandleTradeNoSql.GeneratePartitionKey(symbol, day));

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

        private IMyNoSqlServerDataReader<CandleBidAskNoSql> GetReaderBidAsk(string brokerId, CandleType type)
        {
            if (type == CandleType.Minute)
            {
                if (_minuteBidAskReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _minuteBidAskReaderByBroker[brokerId];
            }

            if (type == CandleType.Hour)
            {
                if (_hourBidAskReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _hourBidAskReaderByBroker[brokerId];
            }

            if (type == CandleType.Day)
            {
                if (_dayBidAskReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _dayBidAskReaderByBroker[brokerId];
            }

            if (type == CandleType.Month)
            {
                if (_monthBidAskReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;
                
                RegisterNoSqlClient(brokerId);
                return _monthBidAskReaderByBroker[brokerId];
            }

            throw new Exception($"Unknown candle type {type}");
        }

        private IMyNoSqlServerDataReader<CandleTradeNoSql> GetReaderTrade(string brokerId, CandleType type)
        {
            if (type == CandleType.Minute)
            {
                if (_minuteTradeReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _minuteTradeReaderByBroker[brokerId];
            }

            if (type == CandleType.Hour)
            {
                if (_hourTradeReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _hourTradeReaderByBroker[brokerId];
            }

            if (type == CandleType.Day)
            {
                if (_dayTradeReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _dayTradeReaderByBroker[brokerId];
            }

            if (type == CandleType.Month)
            {
                if (_monthTradeReaderByBroker.TryGetValue(brokerId, out var reader))
                    return reader;

                RegisterNoSqlClient(brokerId);
                return _monthTradeReaderByBroker[brokerId];
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


                _minuteBidAskReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameMinute(brokerId));
                _hourBidAskReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameHour(brokerId));
                _dayBidAskReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameDay(brokerId));
                _monthBidAskReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleBidAskNoSql>(client, CandleBidAskNoSql.TableNameMonth(brokerId));

                _minuteTradeReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleTradeNoSql>(client, CandleTradeNoSql.TableNameMinute(brokerId));
                _hourTradeReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleTradeNoSql>(client, CandleTradeNoSql.TableNameHour(brokerId));
                _dayTradeReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleTradeNoSql>(client, CandleTradeNoSql.TableNameDay(brokerId));
                _monthTradeReaderByBroker[brokerId] = new MyNoSqlReadRepository<CandleTradeNoSql>(client, CandleTradeNoSql.TableNameMonth(brokerId));
                
                _myNoSqlSubscriberByBroker[brokerId] = client;
            }

            client.Start();

            //todo: use wait initialization

            var index = 0;
            while (index < 50)
            {
                if (_minuteBidAskReaderByBroker[brokerId].Count() > 0
                    && _hourBidAskReaderByBroker[brokerId].Count() > 0
                    && _dayBidAskReaderByBroker[brokerId].Count() > 0
                    && _monthBidAskReaderByBroker[brokerId].Count() > 0
                    && _minuteTradeReaderByBroker[brokerId].Count() > 0
                    && _hourTradeReaderByBroker[brokerId].Count() > 0
                    && _dayTradeReaderByBroker[brokerId].Count() > 0
                    && _monthTradeReaderByBroker[brokerId].Count() > 0)
                {
                    break;
                }

                Thread.Sleep(100);
                index++;
            }
        }
    }
}