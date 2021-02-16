namespace Service.CandlesHistory.Domain.Models
{
    public class CandleValues
    {
        public double Open { get; set; } = 0;
        public double High { get; set; } = 0;
        public double Low { get; set; } = 0;
        public double Close { get; set; } = 0;

        public void Apply(double value)
        {
            if (Open == 0)
                Open = value;

            if (value > High || High == 0)
                High = value;

            if (value < Low || Low == 0)
                Low = value;

            Close = value;
        }
    }
}