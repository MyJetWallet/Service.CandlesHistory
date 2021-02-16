using System;
using System.Collections.Generic;
using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public class CandleBidAskNoSqlWriterManager : ICandleBidAskNoSqlWriterManager
    {
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _minuteWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _hourWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _dayWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();
        private readonly Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>> _monthWriterByBroker = new Dictionary<string, IMyNoSqlServerDataWriter<CandleBidAskNoSql>>();

        public IMyNoSqlServerDataWriter<CandleBidAskNoSql> GetWriter(string brokerId, CandleType type)
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