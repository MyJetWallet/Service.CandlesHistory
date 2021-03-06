﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Microsoft.Extensions.Logging;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public class CandleTradeStoreJob : ICandleTradeStoreJob, IStartable, IDisposable
    {
        private readonly ILogger<CandleTradeStoreJob> _logger;
        private readonly ICandleTradeNoSqlWriterManager _bidAskWriterManager;

        private readonly Dictionary<string, List<CandleTradeNoSql>> _dataToSave = new Dictionary<string, List<CandleTradeNoSql>>();
        private CandleType _candleToSave;

        private readonly CancellationTokenSource _token = new CancellationTokenSource();
        private Task _process;

        public readonly object _gate = new object();

        private Dictionary<CandleType, Dictionary<(string, string, DateTime), CandleTradeNoSql>> _toSave;

        private Dictionary<CandleType, Dictionary<(string, string, DateTime), CandleTradeNoSql>> _toSaveBrokerSymbolType
            = new Dictionary<CandleType, Dictionary<(string, string, DateTime), CandleTradeNoSql>>()
            {
                {CandleType.Minute, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                {CandleType.Hour, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                {CandleType.Day, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                {CandleType.Month, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
            };

        public CandleTradeStoreJob(ILogger<CandleTradeStoreJob> logger, ICandleTradeNoSqlWriterManager bidAskWriterManager)
        {
            _logger = logger;
            _bidAskWriterManager = bidAskWriterManager;
        }

        public void Save(CandleType type, string brokerId, string symbol, CandleTradeNoSql candle)
        {
            if (_token.IsCancellationRequested)
                throw new Exception("CandleTradeStoreJob is stopped");

            lock (_gate)
                _toSaveBrokerSymbolType[type][(brokerId, symbol, candle.Candle.DateTime)] = candle;
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
                _logger.LogInformation("CandleTradeStoreJob is stopped");
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
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Cannot save candles to database, will try next time");
                }

                await Task.Delay(1000);
            }
        }

        private async Task SaveToNoSql()
        {
            if (_toSave == null)
            {
                lock (_gate)
                {
                    _toSave = _toSaveBrokerSymbolType;
                    _toSaveBrokerSymbolType = new Dictionary<CandleType, Dictionary<(string, string, DateTime), CandleTradeNoSql>>()
                    {
                        {CandleType.Minute, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                        {CandleType.Hour, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                        {CandleType.Day, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                        {CandleType.Month, new Dictionary<(string, string, DateTime), CandleTradeNoSql>()},
                    };
                }
            }

            await Save();

            PrepareToSave(CandleType.Minute);

            await Save();

            PrepareToSave(CandleType.Hour);

            await Save();

            PrepareToSave(CandleType.Day);

            await Save();

            PrepareToSave(CandleType.Month);

            await Save();

            _toSave = null;
        }

        private void PrepareToSave(CandleType type)
        {
            _candleToSave = type;

            foreach (var pair in _toSave[type])
            {
                var brokerId = pair.Key.Item1;
                var entity = pair.Value;

                if (!_dataToSave.TryGetValue(brokerId, out var list))
                {
                    list = new List<CandleTradeNoSql>();
                    _dataToSave[brokerId] = list;
                }

                list.Add(entity);
            }

            _toSave[type].Clear();
        }

        private async Task Save()
        {
            foreach (var pair in _dataToSave)
            {
                var writer = _bidAskWriterManager.GetWriter(pair.Key, _candleToSave);
                await writer.BulkInsertOrReplaceAsync(pair.Value);
            }

            _dataToSave.Clear();
        }
    }
}