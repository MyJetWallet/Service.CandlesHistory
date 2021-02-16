using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using DotNetCoreDecorators;
using Grpc.Core.Logging;
using Microsoft.Extensions.Logging;
using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.NoSql;
using Service.CandlesHistory.ServiceBus;
using Service.CandlesHistory.Services;

namespace Service.CandlesHistory.Jobs
{
    public interface ICandleBidAskStore
    {
        void Save(CandleType type, string brokerId, string symbol, CandleBidAskNoSql candle);
    }




    public class CandleCollectorJob: IStartable, IDisposable
    {
        private readonly ILogger<CandleCollectorJob> _logger;
        private readonly IDatabaseClearingJob _clearingJob;
        private readonly Dictionary<(string, string), CandleBidAskNoSql> _toSaveMinuteBrokerSymbolType = new Dictionary<(string, string), CandleBidAskNoSql>();
        private readonly Dictionary<(string, string), CandleBidAskNoSql> _toSaveHourBrokerSymbolType = new Dictionary<(string, string), CandleBidAskNoSql>();
        private readonly Dictionary<(string, string), CandleBidAskNoSql> _toSaveDayBrokerSymbolType = new Dictionary<(string, string), CandleBidAskNoSql>();
        private readonly Dictionary<(string, string), CandleBidAskNoSql> _toSaveMonthBrokerSymbolType = new Dictionary<(string, string), CandleBidAskNoSql>();

        private readonly Dictionary<string, List<CandleBidAskNoSql>> _dataToSave = new Dictionary<string, List<CandleBidAskNoSql>>();
        private CandleType _candleToSave;


        private readonly Dictionary<(string, string, CandleType), CandleBidAskNoSql> _currentBrokerSymbolType = new Dictionary<(string, string, CandleType), CandleBidAskNoSql>();
        
        private readonly CandleFrameSelector _frameSelector = new CandleFrameSelector();

        private readonly CancellationTokenSource _token = new CancellationTokenSource();
        private Task _process;

        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _minuteWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _hourWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _dayWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _monthWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();

        public CandleCollectorJob(ISubscriber<PriceMessage> subscriber, ILogger<CandleCollectorJob> logger, IDatabaseClearingJob clearingJob)
        {
            _logger = logger;
            _clearingJob = clearingJob;
            subscriber.Subscribe(HandlePrice);
        }

        public void Start()
        {
            _process = DoSaveProcess();
        }

        public void Dispose()
        {
            if (!_token.IsCancellationRequested)
                _token.Cancel();

            _process?.Wait();

            try
            {
                SaveToNoSql().Wait();
                _logger.LogInformation("CandleCollectorJob is stopped");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Cannot save candles to database ON STOP");
            }
        }

        private async Task DoSaveProcess()
        {
            while (!_token.IsCancellationRequested)
            {
                try
                {
                    await SaveToNoSql();
                }
                catch(Exception ex)
                {
                    _logger.LogError(ex, "Cannot save candles to database, will try next time");
                }

                await Task.Delay(1000);
            }
        }

        #region save to nosql

        private async Task SaveToNoSql()
        {
            await Save();

            lock (_toSaveMinuteBrokerSymbolType)
            {
                _candleToSave = CandleType.Minute;

                foreach (var pair in _toSaveMinuteBrokerSymbolType)
                {
                    var brokerId = pair.Key.Item1;
                    var entity = pair.Value;

                    if (!_dataToSave.TryGetValue(brokerId, out var list))
                    {
                        list = new List<CandleBidAskNoSql>();
                        _dataToSave[brokerId] = list;
                    }

                    list.Add(entity);
                }

                _toSaveMinuteBrokerSymbolType.Clear();
            }

            await Save();

            lock (_toSaveHourBrokerSymbolType)
            {
                _candleToSave = CandleType.Hour;

                foreach (var pair in _toSaveHourBrokerSymbolType)
                {
                    var brokerId = pair.Key.Item1;
                    var entity = pair.Value;

                    if (!_dataToSave.TryGetValue(brokerId, out var list))
                    {
                        list = new List<CandleBidAskNoSql>();
                        _dataToSave[brokerId] = list;
                    }

                    list.Add(entity);
                }

                _toSaveHourBrokerSymbolType.Clear();
            }

            await Save();

            lock (_toSaveDayBrokerSymbolType)
            {
                _candleToSave = CandleType.Day;

                foreach (var pair in _toSaveDayBrokerSymbolType)
                {
                    var brokerId = pair.Key.Item1;
                    var entity = pair.Value;

                    if (!_dataToSave.TryGetValue(brokerId, out var list))
                    {
                        list = new List<CandleBidAskNoSql>();
                        _dataToSave[brokerId] = list;
                    }

                    list.Add(entity);
                }

                _toSaveDayBrokerSymbolType.Clear();
            }

            await Save();

            lock (_toSaveMonthBrokerSymbolType)
            {
                _candleToSave = CandleType.Month;

                foreach (var pair in _toSaveMonthBrokerSymbolType)
                {
                    var brokerId = pair.Key.Item1;
                    var entity = pair.Value;

                    if (!_dataToSave.TryGetValue(brokerId, out var list))
                    {
                        list = new List<CandleBidAskNoSql>();
                        _dataToSave[brokerId] = list;
                    }

                    list.Add(entity);
                }

                _toSaveMonthBrokerSymbolType.Clear();
            }

            await Save();
        }

        private async Task Save()
        {
            foreach (var pair in _dataToSave)
            {
                var writer = GetWriter(pair.Key, _candleToSave);
                await writer.BulkInsertOrReplaceAsync(pair.Value);
            }

            _dataToSave.Clear();
        }

        #endregion

        private async ValueTask HandlePrice(PriceMessage price)
        {
            Console.WriteLine($"{price.Id}[{price.LiquidityProvider}] {price.Bid}  {price.Ask}");

            //await Task.Delay(50000);
            //throw new Exception("test error");

            if (_token.IsCancellationRequested)
                throw new Exception("CandleCollectorJob is stopped");

            _clearingJob.RegisterBroker(price.LiquidityProvider);

            await HandleMinute(price);
            await HandleHour(price);
            await HandleDay(price);
            await HandleMonth(price);
        }

        #region handler
        
        private async Task HandleMinute(PriceMessage price)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, CandleType.Minute);

            var writer = GetWriter(price.LiquidityProvider, CandleType.Minute);

            CandleBidAskNoSql minute = await GetCurrent(price.LiquidityProvider, price.Id, CandleType.Minute, time, writer);

            if (minute == null || minute.Candle.DateTime != time)
            {
                minute = CandleBidAskNoSql.Create(price.Id, CandleBidAsk.Create(time));
            }

            minute.Candle.Bid.Apply(price.Bid);
            minute.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, CandleType.Minute)] = minute;

            lock (_toSaveMinuteBrokerSymbolType)
                _toSaveMinuteBrokerSymbolType[(price.LiquidityProvider, price.Id)] = minute;
        }

        private async Task HandleHour(PriceMessage price)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, CandleType.Hour);

            var writer = GetWriter(price.LiquidityProvider, CandleType.Hour);

            CandleBidAskNoSql hour = await GetCurrent(price.LiquidityProvider, price.Id, CandleType.Hour, time, writer);

            if (hour == null || hour.Candle.DateTime != time)
            {
                hour = CandleBidAskNoSql.Create(price.Id, CandleBidAsk.Create(time));
            }

            hour.Candle.Bid.Apply(price.Bid);
            hour.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, CandleType.Hour)] = hour;

            lock (_toSaveHourBrokerSymbolType)
                _toSaveHourBrokerSymbolType[(price.LiquidityProvider, price.Id)] = hour;
        }

        private async Task HandleDay(PriceMessage price)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, CandleType.Day);

            var writer = GetWriter(price.LiquidityProvider, CandleType.Day);

            CandleBidAskNoSql day = await GetCurrent(price.LiquidityProvider, price.Id, CandleType.Day, time, writer);

            if (day == null || day.Candle.DateTime != time)
            {
                day = CandleBidAskNoSql.Create(price.Id, CandleBidAsk.Create(time));
            }

            day.Candle.Bid.Apply(price.Bid);
            day.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, CandleType.Day)] = day;

            lock (_toSaveDayBrokerSymbolType)
                _toSaveDayBrokerSymbolType[(price.LiquidityProvider, price.Id)] = day;
        }

        private async Task HandleMonth(PriceMessage price)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, CandleType.Month);

            var writer = GetWriter(price.LiquidityProvider, CandleType.Month);

            CandleBidAskNoSql month = await GetCurrent(price.LiquidityProvider, price.Id, CandleType.Month, time, writer);

            if (month == null || month.Candle.DateTime != time)
            {
                month = CandleBidAskNoSql.Create(price.Id, CandleBidAsk.Create(time));
            }

            month.Candle.Bid.Apply(price.Bid);
            month.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, CandleType.Month)] = month;

            lock (_toSaveMonthBrokerSymbolType)
                _toSaveMonthBrokerSymbolType[(price.LiquidityProvider, price.Id)] = month;
        }

        private async Task<CandleBidAskNoSql> GetCurrent(string brokerId, string symbol, CandleType type, DateTime time, IMyNoSqlServerDataWriter<CandleBidAskNoSql> writer)
        {
            if (_currentBrokerSymbolType.TryGetValue((brokerId, symbol, type), out var entity))
                return entity;

            entity = await writer.GetAsync(CandleBidAskNoSql.GeneratePartitionKey(time), CandleBidAskNoSql.GenerateRowKey(symbol, time));

            return entity;
        }

        #endregion

        private IMyNoSqlServerDataWriter<CandleBidAskNoSql> GetWriter(string brokerId, CandleType type)
        {
            if (type == CandleType.Minute)
            {
                if (_minuteWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleBidAskNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleBidAskNoSql.TableNameMinute(brokerId), true);

                _minuteWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Hour)
            {
                if (_hourWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleBidAskNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleBidAskNoSql.TableNameHour(brokerId), true);

                _hourWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Day)
            {
                if (_dayWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleBidAskNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleBidAskNoSql.TableNameDay(brokerId), true);

                _dayWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Month)
            {
                if (_monthWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleBidAskNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleBidAskNoSql.TableNameMonth(brokerId), true);

                _monthWriterByBroker[brokerId] = writer;

                return writer;
            }

            throw new Exception($"Unknown candle type {type}");
        }

        
    }


}