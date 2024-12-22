import pytest
from nats_queue.nats_limiter import FixedWindowLimiter, IntervalLimiter


@pytest.mark.asyncio
async def test_rate_limiter_initialization():
    max_tasks = 5
    duration = 1
    interval = 2
    limiter = FixedWindowLimiter(max_tasks, duration, interval)
    limiter_interval = IntervalLimiter(interval)

    assert limiter.max == max_tasks
    assert limiter.duration == duration
    assert limiter.count == 0
    assert limiter.interval == interval

    assert limiter_interval.interval == interval


@pytest.mark.asyncio
async def test_rate_limiter_increment():
    max_tasks = 5
    duration = 1
    interval = 2
    limiter = FixedWindowLimiter(max_tasks, duration, interval)
    limiter_interval = IntervalLimiter(interval)

    limiter.inc()
    limiter.inc()
    count = limiter_interval.inc()

    assert limiter.count == 2
    assert count is None


@pytest.mark.asyncio
async def test_rate_limiter_get():
    max_tasks = 5
    duration = 1
    interval = 2
    limiter = FixedWindowLimiter(max_tasks, duration, interval)
    limiter_interval = IntervalLimiter(interval)

    limiter.count = 5
    limiter_interval.count = 5
    assert limiter.get(1) == max_tasks - limiter.count
    assert limiter_interval.get(max_tasks) == max_tasks


@pytest.mark.asyncio
async def test_rate_limiter_timeout_interval():
    max_tasks = 5
    duration = 1
    interval = 5
    limiter = FixedWindowLimiter(max_tasks, duration, interval)
    limiter_interval = IntervalLimiter(interval)

    timeout = limiter.timeout()
    assert timeout == interval

    timeout = limiter_interval.timeout()
    assert timeout == interval


@pytest.mark.asyncio
async def test_rate_limiter_timeout_new_interval():
    max_tasks = 5
    duration = 30
    interval = 1
    limiter = FixedWindowLimiter(max_tasks, duration, interval)

    limiter.timeout()
    limiter.count = 5

    timeout = limiter.timeout()
    assert interval <= timeout


@pytest.mark.asyncio
async def test_rate_limiter_timeout_reset_count():
    max_tasks = 5
    duration = 5
    interval = 2
    limiter = FixedWindowLimiter(max_tasks, duration, interval)

    limiter.count = 5
    limiter.timeout()

    assert limiter.count == 0
