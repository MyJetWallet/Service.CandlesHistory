using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Microsoft.Extensions.Logging;
using Service.CandlesHistory.Domain.Models.NoSql;

namespace Service.CandlesHistory.Jobs
{
    public class DatabaseClearingJob: IDatabaseClearingJob, IStartable, IDisposable
    {
        private readonly ILogger<DatabaseClearingJob> _logger;
        private readonly Dictionary<string, string> _brokers = new Dictionary<string, string>();

        private readonly CancellationTokenSource _token = new CancellationTokenSource();
        private Task _process;

        public DatabaseClearingJob(ILogger<DatabaseClearingJob> logger)
        {
            _logger = logger;
        }

        public void RegisterInstrument(string brokerId, string symbol)
        {
            _brokers[brokerId] = symbol;
        }

        public void Start()
        {
            _process = DoProcess();
        }

        private async Task DoProcess()
        {
            try
            {
                while (!_token.IsCancellationRequested)
                {
                    try
                    {
                        foreach (var pair in _brokers.ToList())
                        {
                            var brokerId = pair.Key;
                            var symbol = pair.Value;

                            var writer = new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleBidAskNoSql>(
                                Program.ReloadedSettings(model => model.MyNoSqlWriterUrl),
                                CandleBidAskNoSql.TableNameMinute(brokerId), true);

                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 1).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 2).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 3).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 4).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 5).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 6).Date), 0);
                            await writer.CleanAndKeepMaxRecords(CandleBidAskNoSql.GeneratePartitionKey(symbol, DateTime.UtcNow.AddDays(-Program.Settings.DaysToKeepMinutes - 7).Date), 0);

                            _logger.LogInformation("Cleanup minutes for broker: {brokerId} and symbol: {symbol}", brokerId, symbol);
                        }
                    }
                    catch(Exception ex)
                    {
                        _logger.LogError(ex, "Cannot cleanup minutes");
                    }

                    await Task.Delay(TimeSpan.FromHours(1), _token.Token);
                }
            }
            catch(Exception)
            { }
        }

        public void Dispose()
        {
            _token.Cancel();
            _process?.Wait();
        }
    }
}