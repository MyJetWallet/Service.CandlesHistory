using SimpleTrading.SettingsReader;

namespace Service.CandlesHistory.Settings
{
    [YamlAttributesOnly]
    public class SettingsModel
    {
        [YamlProperty("CandlesHistory.SeqServiceUrl")]
        public string SeqServiceUrl { get; set; }

        [YamlProperty("CandlesHistory.ServiceBus.HostPort")]
        public string ServiceBusHostPort { get; set; }

        [YamlProperty("CandlesHistory.ServiceBus.TopicName")]
        public string PricesTopicName { get; set; }

        [YamlProperty("CandlesHistory.MyNoSqlWriterUrl")]
        public string MyNoSqlWriterUrl { get; set; }

        [YamlProperty("CandlesHistory.DaysToKeepMinutes")]
        public int DaysToKeepMinutes { get; set; }
    }
}