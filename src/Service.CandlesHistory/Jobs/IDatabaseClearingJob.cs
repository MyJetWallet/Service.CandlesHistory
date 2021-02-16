namespace Service.CandlesHistory.Jobs
{
    public interface IDatabaseClearingJob
    {
        void RegisterBroker(string brokerId);
    }
}