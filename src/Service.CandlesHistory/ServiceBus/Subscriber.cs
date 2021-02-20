using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using DotNetCoreDecorators;
using MyServiceBus.Abstractions;
using MyServiceBus.TcpClient;

namespace Service.CandlesHistory.ServiceBus
{
    public class Subscriber<T> : ISubscriber<T>
    {
        private readonly Func<ReadOnlyMemory<byte>, T> _deserializer;

        public Subscriber(MyServiceBusTcpClient client, string topicName, string queueName, bool deleteOnDisconnect, Func<ReadOnlyMemory<byte>, T> deserializer)
        {
            _deserializer = deserializer;
            client.Subscribe(topicName, queueName, deleteOnDisconnect, Reader);

        }

        private async ValueTask Reader(IReadOnlyList<IMyServiceBusMessage> messages)
        {
            //var size = 0;
            //size = messages.Sum(e => e.Data.Length);
            //Console.WriteLine($"Receive {messages.Count} messages. {(decimal)size/1024/1024} mb");

            //Console.WriteLine($"no: {messages.First().Id} ({messages.First().AttemptNo})");

            //var sw = new Stopwatch();
            //sw.Start();
            var packets = messages.Select(e => _deserializer(e.Data)).ToList();
            //sw.Stop();
            //Console.WriteLine($"Parse for {sw.Elapsed}");

            foreach (var itm in packets)
            {
                foreach (var subscribers in _list)
                    await subscribers(itm);
            }
        }

        private readonly List<Func<T, ValueTask>> _list = new List<Func<T, ValueTask>>();

        public void Subscribe(Func<T, ValueTask> callback)
        {
            _list.Add(callback);
        }

    }
}