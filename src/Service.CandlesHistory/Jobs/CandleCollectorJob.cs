using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using DotNetCoreDecorators;
using Microsoft.Extensions.Logging;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;
using Service.CandlesHistory.ServiceBus;
using Service.CandlesHistory.Services;

namespace Service.CandlesHistory.Jobs
{
    public class CandleCollectorJob
    {
        private readonly ILogger<CandleCollectorJob> _logger;
        private readonly IDatabaseClearingJob _clearingJob;
        private readonly ICandleBidAskStoreJob _storeJob;
        private readonly ICandleBidAskNoSqlWriterManager _bidAskWriterManager;

        private readonly Dictionary<(string, string, CandleType), CandleBidAskNoSql> _currentBrokerSymbolType = new Dictionary<(string, string, CandleType), CandleBidAskNoSql>();
        
        private readonly CandleFrameSelector _frameSelector = new CandleFrameSelector();

        public CandleCollectorJob(ISubscriber<PriceMessage> subscriber, ILogger<CandleCollectorJob> logger, IDatabaseClearingJob clearingJob,
            ICandleBidAskStoreJob storeJob, ICandleBidAskNoSqlWriterManager bidAskWriterManager)
        {
            _logger = logger;
            _clearingJob = clearingJob;
            _storeJob = storeJob;
            _bidAskWriterManager = bidAskWriterManager;
            subscriber.Subscribe(HandlePrice);
        }

        private async ValueTask HandlePrice(PriceMessage price)
        {
            //if (price.Id == "ETHEUR")
            //    Console.WriteLine($"{price.DateTime:yyyy-MM-dd HH:mm:ss} || {price.Id}[{price.LiquidityProvider}] {price.Bid}  {price.Ask}");

            //await Task.Delay(50000);
            //throw new Exception("test error");

            var sw = new Stopwatch();
            sw.Start();

            try
            {
                _clearingJob.RegisterInstrument(price.LiquidityProvider, price.Id);

                await HandleCandleType(price, CandleType.Minute);
                await HandleCandleType(price, CandleType.Hour);
                await HandleCandleType(price, CandleType.Day);
                await HandleCandleType(price, CandleType.Month);
            }
            catch(Exception ex)
            {
                if (ex.Message != "CandleBidAskStoreJob is stopped")
                    _logger.LogError(ex, $"CandleCollectorJob cannot handle price {price.Id}[{price.Id}] {price.DateTime:O} {price.Bid}|{price.Ask}|{price.TradePrice}|{price.TradeVolume}");
            }

            sw.Stop();

            //Console.WriteLine($"handle time: {sw.Elapsed}");
        }

        private async Task HandleCandleType(PriceMessage price, CandleType type)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, type);

            CandleBidAskNoSql candle = GetCurrent(price.LiquidityProvider, price.Id, type) 
                                       ?? await RestoreCurrent(price.LiquidityProvider, price.Id, type, time);

            if (candle == null || candle.Candle.DateTime < time)
            {
                candle = CandleBidAskNoSql.Create(price.Id, CandleBidAsk.Create(time));
            }
            else if (candle.Candle.DateTime > time)
            {
                _logger.LogError($"SKIP PRICE. Receive not actual price datetime. Current time range: {candle.Candle.DateTime:yyyy-MM-dd HH:mm:ss}, price time: {price.DateTime:O}. Price: {price.Id}[{price.LiquidityProvider}] {price.Bid}|{price.Ask}|{price.TradePrice}|{price.TradeVolume}");
                return;
            }

            candle.Candle.Bid.Apply(price.Bid);
            candle.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, type)] = candle;

            _storeJob.Save(type, price.LiquidityProvider, price.Id, candle);
        }

        private CandleBidAskNoSql GetCurrent(string brokerId, string symbol, CandleType type)
        {
            return _currentBrokerSymbolType.TryGetValue((brokerId, symbol, type), out var entity) ? entity : null;
        }

        private async Task<CandleBidAskNoSql> RestoreCurrent(string brokerId, string symbol, CandleType type, DateTime time)
        {
            var writer = _bidAskWriterManager.GetWriter(brokerId, type);
            var entity = await writer.GetAsync(CandleBidAskNoSql.GeneratePartitionKey(symbol, time), CandleBidAskNoSql.GenerateRowKey(time));
            return entity;
        }
    }


}