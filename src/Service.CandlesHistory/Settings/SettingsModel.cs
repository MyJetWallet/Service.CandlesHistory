using SimpleTrading.SettingsReader;

namespace Service.CandlesHistory.Settings
{
    [YamlAttributesOnly]
    public class SettingsModel
    {
        [YamlProperty("CandlesHistory.SeqServiceUrl")]
        public string SeqServiceUrl { get; set; }
    }
}