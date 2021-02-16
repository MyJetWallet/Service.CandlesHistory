using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNetCoreDecorators;
using MyServiceBus.TcpClient;

namespace Service.CandlesHistory.ServiceBus
{
    public class Subscriber<T> : ISubscriber<T>
    {
        public Subscriber(MyServiceBusTcpClient client, string topicName, string queueName, bool deleteOnDisconnect, Func<ReadOnlyMemory<byte>, T> deserializer)
        {
            client.Subscribe(topicName, queueName, deleteOnDisconnect,
                async bytes =>
                {
                    var itm = deserializer(bytes.Data);
                    foreach (var subscribers in _list)
                        await subscribers(itm);
                });
        }

        private readonly List<Func<T, ValueTask>> _list = new List<Func<T, ValueTask>>();

        public void Subscribe(Func<T, ValueTask> callback)
        {
            _list.Add(callback);
        }

    }
}