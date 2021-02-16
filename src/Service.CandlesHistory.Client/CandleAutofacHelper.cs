using Autofac;
using MyNoSqlServer.DataReader;
// ReSharper disable UnusedMember.Global

namespace Service.CandlesHistory.Client
{
    public static class CandleAutofacHelper
    {
        /// <summary>
        /// Register interface ICandleClient
        /// </summary>
        public static void RegisterCandleClients(this ContainerBuilder builder, MyNoSqlSubscriber myNoSqlSubscriber)
        {
            builder
                .RegisterType<CandleClient>()
                .WithParameter("myNoSqlSubscriber", myNoSqlSubscriber)
                .As<ICandleClient>()
                .SingleInstance();
        }
    }
}