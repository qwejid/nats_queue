from datetime import datetime, timedelta
import logging
import nats
from nats.aio.client import Client
from nats.js.api import StreamConfig
from nats.js.errors import BadRequestError
import asyncio
import uuid
import json
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

logger = logging.getLogger("nats")


class Job:
    def __init__(self, queue_name, name, data, timeout=None, delay=0, meta=None):
        if not queue_name or not name:
            raise ValueError("queue_name and name cannot be empty")

        self.id = str(uuid.uuid4())
        self.queue_name = queue_name
        self.name = name
        self.data = data
        self.delay = delay
        self.timeout = timeout
        self.meta = meta or {
            "retry_count": 0,
            "start_time": (
                datetime.now() + timedelta(milliseconds=self.delay)
            ).isoformat(),
            "timeout": self.timeout,
        }

    @property
    def subject(self):
        return f"{self.queue_name}.{self.name}"

    def to_dict(self):
        return {
            "id": self.id,
            "queue_name": self.queue_name,
            "name": self.name,
            "data": self.data,
            "meta": self.meta,
        }


class Queue:
    def __init__(
        self,
        connection: Client,
        topic_name: str,
        priorities: int = 1,
        duplicate_window: int = 0,
    ):
        self.topic_name = topic_name
        self.priorities = priorities
        self.nc = connection
        self.js = None
        self.duplicate_window = duplicate_window

    async def connect(self):
        logger.info("Подключение к JetStream...")
        try:
            self.js = self.nc.jetstream()
            logger.info("Успешно подключено к JetStream")

            subjects = [f"{self.topic_name}.*.*"]
            await self.js.add_stream(
                name=self.topic_name,
                subjects=subjects,
                duplicate_window=self.duplicate_window,
            )
        except BadRequestError:
            await self.js.update_stream(
                name=self.topic_name, duplicate_window=self.duplicate_window
            )
        except Exception as e:
            logger.error(f"Ошибка подключения к NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Закрытие соединения с NATS...")
            await self.nc.close()
            logger.info("Соединение закрыто")

    async def addJob(self, job: Job, priority: int = 1):
        if priority >= self.priorities:
            priority = self.priorities
        elif priority == 0:
            priority = 1
        logger.info(
            f"Добавление задачи {job.subject} в очередь с приоритетом {priority}"
        )
        job_data = json.dumps(job.to_dict()).encode()
        await self.js.publish(
            f"{job.queue_name}.{job.name}.{priority}",
            job_data,
            headers={"Nats-Msg-Id": job.id},
        )

    async def addJobs(self, jobs: list[Job], priority: int = 1):
        for job in jobs:
            await self.addJob(job, priority)


class RateLimiter:
    def __init__(self, max_tasks, duration, concurence):
        self.max_tasks = max_tasks
        self.duration = duration
        self.processed_count = 0
        self.concurence = concurence
        self.start_time = int(time.time() * 1000)
        logger.debug(
            f"RateLimiter создан с max_tasks={max_tasks}, duration={duration}."
        )

    def increment(self, count):
        self.processed_count += count
        logger.debug(f"Увеличение счетчика обработанных задач: {self.processed_count}.")

    async def check_limit(self, active_tasks):
        current_time = int(time.time() * 1000)
        elapsed = current_time - self.start_time

        if elapsed < self.duration and self.processed_count >= self.max_tasks:
            await self._wait_for_limit(elapsed)
        elif elapsed < self.duration and self.processed_count < self.max_tasks:
            free_slot = self.concurence - active_tasks
            if free_slot != 0:
                return free_slot
        elif elapsed > self.duration:
            logger.info(
                f"""Превышено время ожидания лимита обработки.
                Обработано {self.processed_count} из {self.max_tasks}."""
            )
            self._reset_limit()

    async def _wait_for_limit(self, elapsed):
        wait_time = (self.duration - elapsed) / 1000
        logger.info(f"Достижение лимита обработки. Ожидание {wait_time:.2f} мс.")
        await asyncio.sleep(wait_time)
        self._reset_limit()

    def _reset_limit(self):
        self.start_time = int(time.time() * 1000)
        self.processed_count = 0
        logger.debug(
            "Состояние сброшено: start_time обновлен, processed_count сброшен."
        )


class Worker:
    def __init__(
        self,
        connection: Client,
        topic_name: str,
        concurrency: int,
        processor_callback,
        rate_limit: tuple,
        timeout_fetch: int = 5,
        priorities: int = 1,
        max_retries: int = 3,
    ):
        self.nc = connection
        self.topic_name = topic_name
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.processor_callback = processor_callback
        self.priorities = priorities
        self.js = None
        self.max_retries = max_retries
        self.active_tasks = 0
        self.timeout_fetch = timeout_fetch

    async def connect(self):
        logger.info("Подключение воркера к NATS...")
        try:
            self.js = self.nc.jetstream()
            logger.info("Воркер успешно подключен к NATS")
        except Exception as e:
            logger.error(f"Ошибка подключения воркера к NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Закрытие воркера...")
            await self.nc.close()
            logger.info("Воркер успешно отключен")

    async def _process_task(self, msg):
        try:
            self.active_tasks += 1
            job_data = json.loads(msg.data.decode())
            job_start_time = datetime.fromisoformat(job_data["meta"]["start_time"])
            if job_start_time > datetime.now():
                planned_time = job_start_time - datetime.now()
                delay = int(planned_time.total_seconds())
                logger.info(
                    f"""Время для задачи {job_data['name']} еще не наступило.
                    Повторная отправка через {delay}."""
                )
                await msg.nak(delay=delay)
                logger.info(f"Задача {job_data['name']} была повторно отправленна")
                return

            logger.info(f"Обработка задачи {job_data['name']}")

            if job_data.get("meta").get("retry_count") > self.max_retries:
                logger.error(
                    f"Максимальное количество попыток {job_data['name']} превышено."
                )
                await msg.term()
                return

            timeout = job_data["meta"]["timeout"]
            await asyncio.wait_for(
                self.processor_callback(job_data["data"]), timeout=timeout
            )

            await msg.ack_sync()
            logger.info(f"Задача {job_data['name']} успешно обработана.")

        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                logger.error(
                    f"Ошибка при обработке {job_data['name']} истек timeout: {e}",
                    exc_info=True,
                )
            else:
                logger.error(
                    f"Ошибка при обработке задачи {job_data['name']}: {e}",
                    exc_info=True,
                )
            job_data["meta"]["retry_count"] += 1
            job = Job(
                queue_name=job_data["queue_name"],
                name=job_data["name"],
                data=job_data["data"],
                meta=job_data["meta"],
            )
            job_bytes = json.dumps(job.to_dict()).encode()
            await msg.term()
            await self.js.publish(
                msg.subject, job_bytes, headers={"Nats-Msg-Id": job.id}
            )
        finally:
            self.active_tasks -= 1

    async def fetch_messages(self, sub, count):
        try:
            msgs = await sub.fetch(count, timeout=self.timeout_fetch)
            logger.info(f"Получено {len(msgs)}")
            return msgs
        except nats.errors.TimeoutError:
            logger.error("Не удалось получить сообщения: истекло время ожидания.")
        except Exception as e:
            logger.error(f"Ошибка получения сообщений: {e}")

    async def get_subscriptions(self):
        subscriptions = []
        for priority in range(1, self.priorities + 1):
            topic = f"{self.topic_name}.*.{priority}"
            try:
                sub = await self.js.pull_subscribe(
                    topic, durable=f"worker_group_{priority}"
                )
                logger.info(f"Подписка на {topic} успешна: {sub}")
                subscriptions.append(sub)

            except Exception as e:
                logger.error(f"Ошибка подписки на {topic}: {e}")
        return subscriptions

    async def start(self):
        subscriptions = await self.get_subscriptions()

        limiter = RateLimiter(self.rate_limit[0], self.rate_limit[1], self.concurrency)

        while True:
            messages_fetched = False

            for sub in subscriptions:
                free_slot = await limiter.check_limit(self.active_tasks)
                fetch_count = free_slot if free_slot else self.concurrency

                msgs = await self.fetch_messages(sub, fetch_count)

                if msgs:
                    messages_fetched = True

                if messages_fetched:
                    break
                else:
                    logger.info("Нет сообщений для обработки, идем в следующий поток")

            if messages_fetched:

                tasks = [self._process_task(job) for job in msgs]
                asyncio.gather(*tasks)
                limiter.increment(len(tasks))
                await asyncio.sleep(10)
