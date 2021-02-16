using System.Threading;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MyJetWallet.Sdk.Service;
using MyServiceBus.TcpClient;

namespace Service.CandlesHistory
{
    public class ApplicationLifetimeManager : ApplicationLifetimeManagerBase
    {
        private readonly ILogger<ApplicationLifetimeManager> _logger;
        private readonly MyServiceBusTcpClient _busTcpClient;

        public ApplicationLifetimeManager(IHostApplicationLifetime appLifetime, ILogger<ApplicationLifetimeManager> logger,
            MyServiceBusTcpClient busTcpClient)
            : base(appLifetime)
        {
            _logger = logger;
            _busTcpClient = busTcpClient;
        }

        protected override void OnStarted()
        {
            _busTcpClient.Start();
            _logger.LogInformation("OnStarted has been called.");
        }

        protected override void OnStopping()
        {
            _busTcpClient.Stop();
            Thread.Sleep(3000);

            _logger.LogInformation("OnStopping has been called.");
        }

        protected override void OnStopped()
        {
            _logger.LogInformation("OnStopped has been called.");
        }
    }
}
