using MyServiceBus.TcpClient;

namespace Service.CandlesHistory.ServiceBus
{
    public class PriceServiceBusSubscriber : Subscriber<PriceMessage>
    {
        public PriceServiceBusSubscriber(MyServiceBusTcpClient client, string queueName, string topicName) :
            base(client, topicName, queueName, false,
                bytes => bytes.ByteArrayToServiceBusContract())
        {

        }
    }
}