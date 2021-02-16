using Autofac;
using Autofac.Core;
using Autofac.Core.Registration;
using MyNoSqlServer.Abstractions;
using Service.CandlesHistory.Jobs;
using Service.CandlesHistory.NoSql;

namespace Service.CandlesHistory.Modules
{
    public class ServiceModule: Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder
                .RegisterType<CandleCollectorJob>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();

            //builder.Register(ctx => new MyNoSqlServer.DataWriter.MyNoSqlServerDataWriter<CandleNoSql>(
            //        Program.ReloadedSettings(model => model.MyNoSqlWriterUrl), CandleNoSql.TableName, true))
            //    .As<IMyNoSqlServerDataWriter<CandleNoSql>>()
            //    .SingleInstance();


            builder
                .RegisterType<DatabaseClearingJob>()
                .As<IDatabaseClearingJob>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }
    }
}