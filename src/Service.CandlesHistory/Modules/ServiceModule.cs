using Autofac;
using Service.CandlesHistory.Jobs;

namespace Service.CandlesHistory.Modules
{
    public class ServiceModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
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
                .RegisterType<CandleBidAskStore>()
                .As<ICandleBidAskStore>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }
    }
}