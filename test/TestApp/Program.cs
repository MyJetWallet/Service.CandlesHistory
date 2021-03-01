using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MyNoSqlServer.DataReader;
using Service.CandlesHistory.Client;
using Service.CandlesHistory.Domain.Models;

namespace TestApp
{
    class Program
    {
        static async Task Main(string[] args)
        {

            Console.Write("Press enter to start");
            Console.ReadLine();


            var client = new CandleClient(() => "192.168.10.80:5125", "test-app");

            while (true)
            {
                Console.Clear();
                Console.WriteLine("Press enter to START");
                Console.ReadLine();

                PrintCountTrade(client, "jetwallet", "BTCEUR", 10, CandleType.Minute);
                PrintCountTrade(client, "jetwallet", "BTCEUR", 10, CandleType.Minute);
                PrintCountTrade(client, "jetwallet", "BTCEUR", 10, CandleType.Hour);
                PrintCountTrade(client, "jetwallet", "BTCEUR", 10, CandleType.Day);
                PrintCountTrade(client, "jetwallet", "BTCEUR", 10, CandleType.Month);
            }


            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 10, CandleType.Minute);
            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 10, CandleType.Minute);
            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 10, CandleType.Hour);
            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 10, CandleType.Day);
            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 10, CandleType.Month);

            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 100, CandleType.Hour);
            //PrintCountBidAsk(client, "jetwallet", "BTCEUR", 100, CandleType.Minute);

            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-02-17 02:00:00"), DateTime.Parse("2021-02-17 02:20:00"), CandleType.Minute);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-02-17 02:00:00"), DateTime.Parse("2021-02-17 02:20:00"), CandleType.Hour);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-02-17 02:10:00"), DateTime.Parse("2021-02-17 02:20:00"), CandleType.Hour);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-02-17 01:10:00"), DateTime.Parse("2021-02-17 06:20:00"), CandleType.Hour);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-02-17 01:10:00"), DateTime.Parse("2021-02-17 06:20:00"), CandleType.Month);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-01-01 01:10:00"), DateTime.Parse("2021-04-17 06:20:00"), CandleType.Month);
            //PrintRangeBidAsk(client, "jetwallet", "ETHEUR", DateTime.Parse("2021-01-01 01:10:00"), DateTime.Parse("2021-04-17 06:20:00"), CandleType.Day);




            Console.WriteLine("End");
            Console.ReadLine();
        }

        private static void PrintCountBidAsk(CandleClient client, string brokerId, string symbol, int count, CandleType type)
        {
            Console.Clear();
            
            var sw = new Stopwatch();
            sw.Start();
            var list = client.GetLastCandlesBidAskHistoryDesc(brokerId, symbol, count, type).ToArray();
            sw.Stop();

            Console.WriteLine($"{symbol}[{brokerId}] {count} {type} [COUNT]");

            foreach (var candle in list)
            {
                Console.WriteLine(
                    $"{candle.DateTime:yyyy-MM-dd HH:mm:ss}\t{candle.Ask.Open}\t{candle.Ask.High}\t{candle.Ask.Low}\t{candle.Ask.Close}");
            }

            Console.WriteLine($"execute time: {sw.Elapsed}");
            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
        }

        private static void PrintRangeBidAsk(CandleClient client, string brokerId, string symbol, DateTime from, DateTime to, CandleType type)
        {
            Console.Clear();

            var sw = new Stopwatch();
            sw.Start();
            var list = client.GetCandlesBidAskHistoryDesc(brokerId, symbol, from, to, type).ToArray();
            sw.Stop();

            Console.WriteLine($"{symbol}[{brokerId}] {type}  from {from:yyyy-MM-dd HH:mm:ss} to {to:yyyy-MM-dd HH:mm:ss}");

            foreach (var candle in list)
            {
                Console.WriteLine(
                    $"{candle.DateTime:yyyy-MM-dd HH:mm:ss}\t{candle.Ask.Open}\t{candle.Ask.High}\t{candle.Ask.Low}\t{candle.Ask.Close}");
            }

            Console.WriteLine($"execute time: {sw.Elapsed}");
            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
        }

        private static void PrintCountTrade(CandleClient client, string brokerId, string symbol, int count, CandleType type)
        {
            Console.Clear();

            var sw = new Stopwatch();
            sw.Start();
            var list = client.GetLastCandlesTradeHistoryDesc(brokerId, symbol, count, type).ToArray();
            sw.Stop();

            Console.WriteLine($"{symbol}[{brokerId}] {count} {type} [COUNT][TRADE]");

            foreach (var candle in list)
            {
                Console.WriteLine($"{candle.DateTime:yyyy-MM-dd HH:mm:ss}\t{candle.Trade.Open}\t{candle.Trade.High}\t{candle.Trade.Low}\t{candle.Trade.Close} | {candle.Volume}");
            }

            Console.WriteLine($"execute time: {sw.Elapsed}");
            Console.WriteLine("Press enter to continue...");
            Console.ReadLine();
        }
    }
}
