using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNetCoreDecorators;
using Microsoft.Extensions.Logging;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;
using Service.CandlesHistory.ServiceBus;
using Service.CandlesHistory.Services;

namespace Service.CandlesHistory.Jobs
{
    public class CandleTradeCollectorJob
    {
        private readonly ILogger<CandleTradeCollectorJob> _logger;
        private readonly IDatabaseClearingJob _clearingJob;
        private readonly ICandleTradeStoreJob _tradeStoreJob;
        private readonly ICandleTradeNoSqlWriterManager _tradeWriterManager;

        private readonly Dictionary<(string, string, CandleType), CandleTradeNoSql> _currentBrokerSymbolType = new Dictionary<(string, string, CandleType), CandleTradeNoSql>();
        
        private readonly CandleFrameSelector _frameSelector = new CandleFrameSelector();

        public CandleTradeCollectorJob(ISubscriber<PriceMessage> subscriber, ILogger<CandleTradeCollectorJob> logger, IDatabaseClearingJob clearingJob,
            ICandleTradeStoreJob tradeStoreJob,
            ICandleTradeNoSqlWriterManager tradeWriterManager)
        {
            _logger = logger;
            _clearingJob = clearingJob;
            _tradeStoreJob = tradeStoreJob;
            _tradeWriterManager = tradeWriterManager;
            subscriber.Subscribe(HandlePrice);
            
        }

        private async ValueTask HandlePrice(PriceMessage price)
        {
          
            if (price.Price <= 0 || price.Volume <= 0)
                return;

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
                if (ex.Message != "CandleTradeCollectorJob is stopped")
                    _logger.LogError(ex, $"CandleTradeCollectorJob cannot handle price {price.Id}[{price.Id}] {price.DateTime:O} {price.Bid}|{price.Ask}|{price.Price}|{price.Volume}");
            }
        }

        private async Task HandleCandleType(PriceMessage price, CandleType type)
        {
            var time = _frameSelector.SelectFrame(price.DateTime, type);

            CandleTradeNoSql candle = GetCurrent(price.LiquidityProvider, price.Id, type) 
                                       ?? await RestoreCurrent(price.LiquidityProvider, price.Id, type, time);

            if (candle == null || candle.Candle.DateTime < time)
            {
                candle = CandleTradeNoSql.Create(price.Id, CandleTrade.Create(time));
            }
            else if (candle.Candle.DateTime > time)
            {
                _logger.LogError($"SKIP PRICE. Receive not actual price datetime. Current time range: {candle.Candle.DateTime:yyyy-MM-dd HH:mm:ss}, price time: {price.DateTime:O}. Price: {price.Id}[{price.LiquidityProvider}] {price.Bid}|{price.Ask}|{price.Price}|{price.Volume}");
                return;
            }

            candle.Candle.Trade.Apply(price.Price);
            candle.Candle.Volume += price.Volume;

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, type)] = candle;

            _tradeStoreJob.Save(type, price.LiquidityProvider, price.Id, candle);
        }

        private CandleTradeNoSql GetCurrent(string brokerId, string symbol, CandleType type)
        {
            return _currentBrokerSymbolType.TryGetValue((brokerId, symbol, type), out var entity) ? entity : null;
        }

        private async Task<CandleTradeNoSql> RestoreCurrent(string brokerId, string symbol, CandleType type, DateTime time)
        {
            var writer = _tradeWriterManager.GetWriter(brokerId, type);
            var entity = await writer.GetAsync(CandleTradeNoSql.GeneratePartitionKey(symbol, time), CandleTradeNoSql.GenerateRowKey(time));
            return entity;
        }
    }


}