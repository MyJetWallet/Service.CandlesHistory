using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public class CandleTradeNoSqlWriterManager : ICandleTradeNoSqlWriterManager
    {
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>> _minuteWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>> _hourWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>> _dayWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>> _monthWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleTradeNoSql>>();

        public IMyNoSqlServerDataWriter<CandleTradeNoSql> GetWriter(string brokerId, CandleType type)
        {
            if (type == CandleType.Minute)
            {
                if (_minuteWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleTradeNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleTradeNoSql.TableNameMinute(brokerId), true);

                _minuteWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Hour)
            {
                if (_hourWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleTradeNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleTradeNoSql.TableNameHour(brokerId), true);

                _hourWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Day)
            {
                if (_dayWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleTradeNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleTradeNoSql.TableNameDay(brokerId), true);

                _dayWriterByBroker[brokerId] = writer;

                return writer;
            }

            if (type == CandleType.Month)
            {
                if (_monthWriterByBroker.TryGetValue(brokerId, out var writer))
                    return writer;

                writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleTradeNoSql>(
                    Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                    CandleTradeNoSql.TableNameMonth(brokerId), true);

                _monthWriterByBroker[brokerId] = writer;

                return writer;
            }

            throw new Exception($"Unknown candle type {type}");
        }
    }
}