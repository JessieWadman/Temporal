namespace Temporal.Internal;

// Helps controlling the time in unit tests 
public interface ISystemClock
{
    DateTimeOffset NowWithOffset { get; }

    DateTime UtcNow => NowWithOffset.ToUniversalTime().DateTime;
    DateTimeOffset UtcNowWithOffset => NowWithOffset.ToUniversalTime();
    DateOnly UtcToday => DateOnly.FromDateTime(UtcNow);
}

public class SystemClock : ISystemClock
{
    public DateTimeOffset NowWithOffset => DateTimeOffset.Now;
}

public class MockSystemClock : ISystemClock
{
    public DateTimeOffset NowWithOffset { get; set; } = DateTimeOffset.Now;
}