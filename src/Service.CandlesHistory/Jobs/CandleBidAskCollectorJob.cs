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
    public class CandleBidAskCollectorJob
    {
        private readonly ILogger<CandleBidAskCollectorJob> _logger;
        private readonly IDatabaseClearingJob _clearingJob;
        private readonly ICandleBidAskStoreJob _bitaskStoreJob;
        private readonly ICandleTradeStoreJob _tradeStoreJob;
        private readonly ICandleBidAskNoSqlWriterManager _bidAskWriterManager;
        private readonly ICandleTradeNoSqlWriterManager _tradeWriterManager;

        private readonly Dictionary<(string, string, CandleType), CandleBidAskNoSql> _currentBrokerSymbolType = new Dictionary<(string, string, CandleType), CandleBidAskNoSql>();
        
        private readonly CandleFrameSelector _frameSelector = new CandleFrameSelector();

        public CandleBidAskCollectorJob(ISubscriber<PriceMessage> subscriber, ILogger<CandleBidAskCollectorJob> logger, IDatabaseClearingJob clearingJob,
            ICandleBidAskStoreJob bitaskStoreJob,
            ICandleTradeStoreJob tradeStoreJob,
            ICandleBidAskNoSqlWriterManager bidAskWriterManager,
            ICandleTradeNoSqlWriterManager tradeWriterManager)
        {
            _logger = logger;
            _clearingJob = clearingJob;
            _bitaskStoreJob = bitaskStoreJob;
            _tradeStoreJob = tradeStoreJob;
            _bidAskWriterManager = bidAskWriterManager;
            _tradeWriterManager = tradeWriterManager;
            subscriber.Subscribe(HandlePrice);
            
        }

        private async ValueTask HandlePrice(PriceMessage price)
        {
            if (price.Ask <= 0 && price.Bid <= 0)
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
                if (ex.Message != "CandleBidAskStoreJob is stopped")
                    _logger.LogError(ex, $"CandleBidAskCollectorJob cannot handle price {price.Id}[{price.Id}] {price.DateTime:O} {price.Bid}|{price.Ask}|{price.Price}|{price.Volume}");
            }
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
                _logger.LogError($"SKIP PRICE. Receive not actual price datetime. Current time range: {candle.Candle.DateTime:yyyy-MM-dd HH:mm:ss}, price time: {price.DateTime:O}. Price: {price.Id}[{price.LiquidityProvider}] {price.Bid}|{price.Ask}|{price.Price}|{price.Volume}");
                return;
            }

            candle.Candle.Bid.Apply(price.Bid);
            candle.Candle.Ask.Apply(price.Ask);

            _currentBrokerSymbolType[(price.LiquidityProvider, price.Id, type)] = candle;

            _bitaskStoreJob.Save(type, price.LiquidityProvider, price.Id, candle);
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