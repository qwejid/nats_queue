import pytest
import time
from nats_queue.main import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_initialization():
    max_tasks = 5
    duration = 10
    concurence = 3
    limiter = RateLimiter(max_tasks, duration, concurence)

    assert limiter.max_tasks == max_tasks
    assert limiter.duration == duration
    assert limiter.processed_count == 0
    assert limiter.start_time <= int(time.time() * 1000)


@pytest.mark.asyncio
async def test_rate_limiter_increment():
    max_tasks = 5
    duration = 10
    concurence = 3
    limiter = RateLimiter(max_tasks, duration, concurence)

    for _ in range(3):
        limiter.increment(1)

    assert limiter.processed_count == 3


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_no_wait():
    max_tasks = 5
    duration = 2
    concurence = 3
    limiter = RateLimiter(max_tasks, duration, concurence)

    limiter.increment(1)

    start_time = int(time.time() * 1000)
    await limiter.check_limit(1)
    end_time = int(time.time() * 1000)

    assert end_time - start_time < 2
    assert limiter.processed_count == 1


@pytest.mark.asyncio
async def test_rate_limiter_check_limit_with_wait():
    max_tasks = 2
    duration = 2
    concurence = 3
    limiter = RateLimiter(max_tasks, duration, concurence)

    for _ in range(max_tasks):
        limiter.increment(1)

    start_time = int(time.time() * 1000)
    await limiter.check_limit(1)
    end_time = int(time.time() * 1000)

    assert end_time - start_time >= duration
    assert limiter.processed_count == 0


@pytest.mark.asyncio
async def test_rate_limiter_reset():
    max_tasks = 3
    duration = 1
    concurence = 3
    limiter = RateLimiter(max_tasks, duration, concurence)

    for _ in range(max_tasks):
        limiter.increment(1)

    await limiter.check_limit(1)

    assert limiter.processed_count == 0
    assert limiter.start_time > int(time.time() * 1000) - duration
