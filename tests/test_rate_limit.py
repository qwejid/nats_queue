import asyncio
import pytest
import time
from nats_queue.nats_limiter import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_initialization():
    max_tasks = 5
    duration = 1000
    concurrency = 2
    interval = 200
    limiter = RateLimiter(max_tasks, duration, concurrency, interval)

    assert limiter.limiter.max_tasks == max_tasks
    assert limiter.limiter.duration == duration
    assert limiter.limiter.count == 0
    assert limiter.limiter.interval == interval
    assert limiter.concurrency == concurrency


@pytest.mark.asyncio
async def test_rate_limiter_increment():
    max_tasks = 5
    duration = 1000
    concurrency = 3
    interval = 200
    limiter = RateLimiter(max_tasks, duration, concurrency, interval)

    limiter.limiter.inc()
    limiter.limiter.inc()
    limiter.limiter.inc(2)

    assert limiter.limiter.count == 4


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_with_wait():
    max_tasks = 1
    duration = 10000
    concurrency = 3
    interval = 10
    limiter = RateLimiter(max_tasks, duration, concurrency, interval)

    limiter.limiter.inc()
    await limiter.check_limit()
    await asyncio.sleep(1)

    limiter.limiter.inc(2)
    start_time = time.time()
    await limiter.check_limit()
    end_time = time.time()

    elapsed_time = end_time - start_time
    expected_wait_time = duration / 1000
    tolerance = 1000
    assert (
        expected_wait_time - tolerance <= elapsed_time <= expected_wait_time + tolerance
    )


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_no_wait():
    max_tasks = 2
    duration = 5000
    concurrency = 1
    interval = 200
    limiter = RateLimiter(max_tasks, duration, concurrency, interval)

    limiter.limiter.inc(2)
    await limiter.check_limit()
    await asyncio.sleep(1)
    assert limiter.limiter.get() == max_tasks - limiter.limiter.count

    start_time = time.time()
    await limiter.check_limit()
    end_time = time.time()
    assert interval / 1000 - 2 <= end_time - start_time <= interval / 1000 + 2


@pytest.mark.asyncio
async def test_rate_limiter_reset():
    max_tasks = 3
    duration = 1000
    concurrency = 2
    interval = 200
    limiter = RateLimiter(max_tasks, duration, concurrency, interval)

    limiter.limiter.inc()
    limiter.limiter.inc()
    limiter.limiter.inc()

    await asyncio.sleep(duration / 1000)
    await limiter.check_limit()

    assert limiter.limiter.count == 0
