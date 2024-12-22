from abc import ABC, abstractmethod
import time


class Limiter(ABC):
    @abstractmethod
    def inc(self) -> None:
        """Increment the internal state."""
        pass

    @abstractmethod
    def timeout(self) -> int:
        """Return the timeout value in milliseconds."""
        pass

    @abstractmethod
    def get(self, max_value: int) -> int:
        """Return the available slots up to the maximum value."""
        pass


class IntervalLimiter(Limiter):
    def __init__(self, interval: int):
        """
        Interval limiter.
        :param interval: Minimum interval between tasks in milliseconds.
        """
        self.interval = interval

    def timeout(self) -> int:
        """Always return the fixed interval."""
        return self.interval

    def inc(self) -> None:
        """No operation (NOOP)."""
        pass

    def get(self, max_value: int) -> int:
        """Always return the maximum value."""
        return max_value


class FixedWindowLimiter(Limiter):
    def __init__(self, max: int, duration: int, interval: int):
        """
        Fixed window rate limiter.
        :param max_tasks: Maximum number of tasks per window.
        :param duration: Window duration in milliseconds.
        :param interval: Minimum interval between tasks in milliseconds.
        """
        self.max = max
        self.duration = duration
        self.interval = interval
        self.count = 0
        self.timestamp = 0

    def timeout(self) -> int:
        """
        Calculates the waiting time for the next task.
        :return: Waiting time in milliseconds.
        """

        now = int(time.time())
        timestamp = now - (now % (self.duration))

        if timestamp != self.timestamp:
            self.count = 0
            self.timestamp = timestamp

        if self.count >= self.max:
            self.count = 0
            self.timestamp = timestamp + self.duration
            return max(self.timestamp - now, self.interval)

        return self.interval

    def inc(self) -> None:
        """
        Increments the task counter.
        """
        self.count += 1

    def get(self, max_value: int) -> int:
        """
        Returns the available slots for tasks.
        :return: Minimum of max_value and remaining slots in the window.
        """
        return min(max_value, self.max - self.count)
