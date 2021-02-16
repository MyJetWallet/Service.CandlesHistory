using System;
using NUnit.Framework;
using Service.CandlesHistory.Domain.Models;
using Service.CandlesHistory.Services;

namespace Service.CandlesHistory.Tests
{
    public class CandleFrameSelectorTest
    {
        private readonly CandleFrameSelector _selector = new CandleFrameSelector();

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Test1()
        {
            var time = DateTime.Parse("2021-02-15 15:00:59");
            var candle = CandleType.Minute;
            var expectedFrame = DateTime.Parse("2021-02-15 15:00:00");
            var frame = _selector.SelectFrame(time, candle);
            Assert.AreEqual(expectedFrame, frame, $"{time:O} to {candle}");
        }

        [TestCase("2021-02-15 15:35:23", "2021-02-15 15:35:00")]
        [TestCase("2021-02-15 15:00:00", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:00:01", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:00:59", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:01:00", "2021-02-15 15:01:00")]
        [TestCase("2021-02-15 00:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 23:59:59", "2021-02-15 23:59:00")]
        public void Minute(string timeStr, string expectedStr)
        {
            var candle = CandleType.Minute;
            var time = DateTime.Parse(timeStr);
            var expectedFrame = DateTime.Parse(expectedStr);
            var frame = _selector.SelectFrame(time, candle);
            Assert.AreEqual(expectedFrame, frame, $"{time:O} to {candle}");
        }

        [TestCase("2021-02-15 15:35:23", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:00:00", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:00:01", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:00:59", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 15:01:00", "2021-02-15 15:00:00")]
        [TestCase("2021-02-15 00:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 23:59:59", "2021-02-15 23:00:00")]
        [TestCase("2021-02-15 00:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-01-01 03:23:19", "2021-01-01 03:00:00")]
        public void Hour(string timeStr, string expectedStr)
        {
            var candle = CandleType.Hour;
            var time = DateTime.Parse(timeStr);
            var expectedFrame = DateTime.Parse(expectedStr);
            var frame = _selector.SelectFrame(time, candle);
            Assert.AreEqual(expectedFrame, frame, $"{time:O} to {candle}");
        }

        [TestCase("2021-02-15 15:35:23", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 15:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 15:00:01", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 15:00:59", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 15:01:00", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 00:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 23:59:59", "2021-02-15 00:00:00")]
        [TestCase("2021-02-15 00:00:00", "2021-02-15 00:00:00")]
        [TestCase("2021-01-01 03:23:19", "2021-01-01 00:00:00")]
        [TestCase("2021-02-28 12:41:30", "2021-02-28 00:00:00")]
        [TestCase("2021-01-31 03:23:19", "2021-01-31 00:00:00")]
        public void Day(string timeStr, string expectedStr)
        {
            var candle = CandleType.Day;
            var time = DateTime.Parse(timeStr);
            var expectedFrame = DateTime.Parse(expectedStr);
            var frame = _selector.SelectFrame(time, candle);
            Assert.AreEqual(expectedFrame, frame, $"{time:O} to {candle}");
        }

        [TestCase("2021-02-15 15:35:23", "2021-02-01 00:00:00")]
        [TestCase("2021-02-15 15:00:00", "2021-02-01 00:00:00")]
        [TestCase("2021-04-15 15:00:01", "2021-04-01 00:00:00")]
        [TestCase("2021-02-15 15:00:59", "2021-02-01 00:00:00")]
        [TestCase("2021-02-15 15:01:00", "2021-02-01 00:00:00")]
        [TestCase("2021-11-15 00:00:00", "2021-11-01 00:00:00")]
        [TestCase("2021-02-15 23:59:59", "2021-02-01 00:00:00")]
        [TestCase("2020-02-15 00:00:00", "2020-02-01 00:00:00")]
        [TestCase("2021-01-01 03:23:19", "2021-01-01 00:00:00")]
        [TestCase("2021-02-28 12:41:30", "2021-02-01 00:00:00")]
        [TestCase("2021-01-31 03:23:19", "2021-01-01 00:00:00")]
        [TestCase("2021-01-23 12:41:30", "2021-01-01 00:00:00")]
        [TestCase("2021-12-31 03:23:19", "2021-12-01 00:00:00")]
        public void Month(string timeStr, string expectedStr)
        {
            var candle = CandleType.Month;
            var time = DateTime.Parse(timeStr);
            var expectedFrame = DateTime.Parse(expectedStr);
            var frame = _selector.SelectFrame(time, candle);
            Assert.AreEqual(expectedFrame, frame, $"{time:O} to {candle}");
        }
    }
}
