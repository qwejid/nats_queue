import asyncio
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


class Limiter:
    def inc(self):
        """Increments the task counter."""
        raise NotImplementedError

    def timeout(self) -> float:
        """Returns the timeout before the next task can proceed."""
        raise NotImplementedError

    def get(self) -> int:
        """Returns the available slots for tasks."""
        raise NotImplementedError


class FixedWindowLimiter:
    def __init__(self, max_tasks: int, duration: int, interval: int) -> None:
        """
        Fixed window rate limiter.
        :param max_tasks: Maximum number of tasks per window.
        :param duration: Window duration in milliseconds.
        :param interval: Minimum interval between tasks in milliseconds.
        """
        self.max_tasks = max_tasks
        self.duration = duration
        self.interval = interval
        self.count = 0
        self.timestamp = 0

        logger.info(
            f"FixedWindowLimiter initialized with max_tasks={max_tasks}, "
            f"duration={duration}ms, interval={interval}ms."
        )

    def timeout(self) -> float:
        """
        Calculates the waiting time for the next task.
        :return: Waiting time in seconds.
        """

        now = int(time.time() * 1000)

        if now >= self.timestamp + self.duration:
            self.timestamp = now - (now % self.duration)
            self.count = 0
            logger.debug(f"New window started: {self.timestamp}")

        if self.count >= self.max_tasks:
            wait_time = (self.timestamp + self.duration - now) / 1000
            logger.debug(f"Task limit exceeded. Waiting for {wait_time:.2f} seconds.")
            return wait_time

        return self.interval / 1000

    def inc(self, new_task_count=1) -> None:
        """
        Increments the task counter.
        """
        self.count += new_task_count
        logger.debug(
            f"Task counter incremented by {new_task_count}. "
            f"Current count: {self.count}."
        )

    def get(self) -> int:
        """
        Returns the available slots for tasks.
        :return: Number of available slots.
        """
        available_slots = self.max_tasks - self.count
        logger.debug(f"{available_slots} slots available for tasks.")
        return available_slots


class RateLimiter:
    def __init__(
        self, max_tasks: int, duration: int, concurrency: int, interval: int
    ) -> None:
        """
        RateLimiter to manage task execution rate.
        :param max_tasks: Maximum number of tasks per window.
        :param duration: Window duration in milliseconds.
        :param concurrency: Maximum number of concurrent tasks.
        :param interval: Minimum interval between tasks in milliseconds.
        """
        self.limiter = FixedWindowLimiter(max_tasks, duration, interval)
        self.concurrency = concurrency
        logger.debug(
            f"RateLimiter initialized with max_tasks={max_tasks}, duration={duration}, "
            f"concurrency={concurrency}, interval={interval}."
        )

    async def check_limit(self) -> None:
        """
        Checks rate limits before executing a task.
        Waits if the limits are exceeded.
        """
        while True:
            free_slots = self.limiter.get()
            if free_slots > 0:
                break
            wait_time = self.limiter.timeout()
            if wait_time > 0:
                logger.debug(f"Limit reached. Waiting {wait_time:.2f} seconds.")
                await asyncio.sleep(wait_time)
