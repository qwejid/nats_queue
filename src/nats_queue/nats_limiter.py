import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class Limiter:
    def inc(self):
        raise NotImplementedError

    def timeout(self):
        raise NotImplementedError

    def get(self):
        raise NotImplementedError


class FixedWindowLimiter:
    def __init__(self, max_tasks: int, duration: int, interval: int) -> None:
        """
        Лимитер фиксированного окна.
        :param max_tasks: Максимальное количество задач в окно.
        :param duration: Длительность окна в миллисекундах.
        :param interval: Минимальный интервал между задачами в миллисекундах.
        """
        self.max_tasks = max_tasks
        self.duration = duration
        self.interval = interval
        self.count = 0
        self.timestamp = 0

    def timeout(self) -> float:
        """
        Вычисляет время ожидания для следующей задачи.
        :return: Время ожидания в секундах.
        """
        now = int(time.time() * 1000)
        window_start = now - (now % self.duration)

        if window_start != self.timestamp:
            self.count = 0
            self.timestamp = window_start

        if self.count >= self.max_tasks:
            self.count = 0
            self.timestamp = window_start + self.duration
            wait_time = max(self.timestamp - now, self.interval) / 1000
            logger.info(f"Лимит задач превышен. Ожидание {wait_time:.2f} секунд.")
            return wait_time

        return self.interval / 1000

    def inc(self) -> None:
        """
        Увеличивает счётчик выполненных задач.
        """
        self.count += 1

    def get(self) -> int:
        """
        Возвращает доступное количество слотов для выполнения задач.
        :param max_tasks: Максимальное количество задач, переданное вызывающим кодом.
        :return: Число доступных слотов.
        """
        return self.max_tasks - self.count


class RateLimiter:
    def __init__(
        self, max_tasks: int, duration: int, concurrency: int, interval: int
    ) -> None:
        """
        RateLimiter для управления частотой выполнения задач.
        :param max_tasks: Максимальное количество задач в окно.
        :param duration: Длительность окна в миллисекундах.
        :param concurrency: Максимальное количество одновременно выполняемых задач.
        :param interval: Минимальный интервал между задачами в миллисекундах.
        """
        self.limiter = FixedWindowLimiter(max_tasks, duration, interval)
        self.concurrency = concurrency
        logger.debug(
            f"RateLimiter создан с max_tasks={max_tasks}, duration={duration}, "
            f"concurrency={concurrency}, interval={interval}."
        )

    async def check_limit(self) -> None:
        """
        Проверяет ограничения перед выполнением задачи.
        Ждет, если лимиты превышены.
        """
        while True:
            free_slots = self.limiter.get()
            if free_slots > 0:
                break
            wait_time = self.limiter.timeout()
            if wait_time > 0:
                logger.info(f"Достигнут лимит. Ожидание {wait_time:.2f} секунд.")
                await asyncio.sleep(wait_time)
