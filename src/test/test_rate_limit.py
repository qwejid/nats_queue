import pytest
import time
from nats_queue.main import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_initialization():
    max_tasks = 5
    duration = 10
    limiter = RateLimiter(max_tasks, duration)

    assert limiter.max_tasks == max_tasks
    assert limiter.duration == duration
    assert limiter.processed_count == 0
    assert limiter.start_time <= int(time.time() * 1000)


@pytest.mark.asyncio
async def test_rate_limiter_increment():
    max_tasks = 5
    duration = 10
    limiter = RateLimiter(max_tasks, duration)

    for _ in range(3):
        limiter.increment()

    assert limiter.processed_count == 3


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_no_wait():
    max_tasks = 5
    duration = 2
    limiter = RateLimiter(max_tasks, duration)

    limiter.increment()

    start_time = int(time.time() * 1000)
    await limiter.check_limit()
    end_time = int(time.time() * 1000)

    assert end_time - start_time < 2
    assert limiter.processed_count == 1


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_with_wait():
    max_tasks = 2
    duration = 2
    limiter = RateLimiter(max_tasks, duration)

    for _ in range(max_tasks):
        limiter.increment()

    start_time = int(time.time() * 1000)
    await limiter.check_limit()
    end_time = int(time.time() * 1000)

    assert end_time - start_time >= duration
    assert limiter.processed_count == 0


@pytest.mark.asyncio
async def test_rate_limiter_reset():
    max_tasks = 3
    duration = 1
    limiter = RateLimiter(max_tasks, duration)

    for _ in range(max_tasks):
        limiter.increment()

    await limiter.check_limit()

    assert limiter.processed_count == 0
    assert limiter.start_time > int(time.time() * 1000) - duration
