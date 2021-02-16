namespace Service.CandlesHistory.Jobs
{
    public interface IDatabaseClearingJob
    {
        void RegisterInstrument(string brokerId, string symbol);
    }
}