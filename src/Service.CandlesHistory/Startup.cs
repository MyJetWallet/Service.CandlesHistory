using System.Reflection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Autofac;
using DotNetCoreDecorators;
using MyJetWallet.Sdk.GrpcMetrics;
using MyJetWallet.Sdk.GrpcSchema;
using MyJetWallet.Sdk.Service;
using MyServiceBus.TcpClient;
using Prometheus;
using ProtoBuf.Grpc.Server;
using Service.CandlesHistory.Grpc;
using Service.CandlesHistory.Modules;
using Service.CandlesHistory.ServiceBus;
using Service.CandlesHistory.Services;
using SimpleTrading.BaseMetrics;
using SimpleTrading.ServiceStatusReporterConnector;

namespace Service.CandlesHistory
{
    public class Startup
    {
        private readonly MyServiceBusTcpClient _serviceBusClient;

        public Startup()
        {
            var appName = ApplicationEnvironment.HostName ?? ApplicationEnvironment.AppName;
            _serviceBusClient = new MyServiceBusTcpClient(Program.ReloadedSettings(model => model.ServiceBusHostPort), appName);
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddCodeFirstGrpc(options =>
            {
                options.Interceptors.Add<PrometheusMetricsInterceptor>();
                options.BindMetricsInterceptors();
            });

            services.AddHostedService<ApplicationLifetimeManager>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseMetricServer();

            app.BindServicesTree(Assembly.GetExecutingAssembly());

            app.BindIsAlive();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcSchema<HelloService, IHelloService>();

                endpoints.MapGrpcSchemaRegistry();

                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
                });
            });

            //_serviceBusClient.Start();
        }

        public void ConfigureContainer(ContainerBuilder builder)
        {
            builder.RegisterModule<SettingsModule>();
            builder.RegisterModule<ServiceModule>();

            builder.RegisterInstance(new PriceServiceBusSubscriber(_serviceBusClient, "Candles-History", Program.Settings.PricesTopicName))
                .As<ISubscriber<PriceMessage>>()
                .SingleInstance();

            builder.RegisterInstance(_serviceBusClient).AsSelf().SingleInstance();
        }
    }
}
