using Autofac;
using DotNetCoreDecorators;
using MyJetWallet.Sdk.Service;
using MyServiceBus.TcpClient;
using Service.CandlesHistory.Jobs;
using Service.CandlesHistory.ServiceBus;

namespace Service.CandlesHistory.Modules
{
    public class ServiceModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            var appName = ApplicationEnvironment.HostName ?? ApplicationEnvironment.AppName;
            var serviceBusClient = new MyServiceBusTcpClient(Program.ReloadedSettings(model => model.ServiceBusHostPort), appName);
            builder.RegisterInstance(serviceBusClient).AsSelf().SingleInstance();

            builder.RegisterInstance(new PriceServiceBusSubscriber(serviceBusClient, "Candles-History-1", Program.Settings.PricesTopicName))
                .As<ISubscriber<PriceMessage>>()
                .SingleInstance();


            builder
                .RegisterType<CandleCollectorJob>()
                .AutoActivate()
                .SingleInstance();

            builder
                .RegisterType<DatabaseClearingJob>()
                .As<IDatabaseClearingJob>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();

            builder
                .RegisterType<CandleBidAskNoSqlWriterManager>()
                .As<ICandleBidAskNoSqlWriterManager>()
                .SingleInstance();

            builder
                .RegisterType<CandleBidAskStoreJob>()
                .As<ICandleBidAskStoreJob>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }
    }
}