from datetime import datetime, timedelta
import logging
import nats
import asyncio
import uuid
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

logger = logging.getLogger("nats")

user = os.environ.get("NATS_USER")
password = os.environ.get("NATS_PASSWORD")


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
    def __init__(self, topic_name: str, priorities: int = 1):
        self.topic_name = topic_name
        self.priorities = priorities
        self.nc = None
        self.js = None

    async def connect(self):
        logger.info("Подключение к NATS...")
        try:
            self.nc = await nats.connect(
                servers=["nats://localhost:4222"], user=user, password=password
            )
            self.js = self.nc.jetstream()
            logger.info("Успешно подключено к NATS")

            subjects = [f"{self.topic_name}.*.*"]
            await self.js.add_stream(name=self.topic_name, subjects=subjects)
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
    def __init__(self, max_tasks, duration):
        self.max_tasks = max_tasks
        self.duration = duration
        self.processed_count = 0
        self.start_time = int(time.time() * 1000)
        logger.debug(
            f"RateLimiter создан с max_tasks={max_tasks}, duration={duration}."
        )

    def increment(self):
        self.processed_count += 1
        logger.debug(f"Увеличение счетчика обработанных задач: {self.processed_count}.")

    async def check_limit(self):
        current_time = int(time.time() * 1000)
        elapsed = current_time - self.start_time

        if elapsed < self.duration and self.processed_count >= self.max_tasks:
            await self._wait_for_limit(elapsed)
        elif elapsed > self.duration:
            logger.info(
                f"Превышено время ожидания лимита обработки. Обработано {self.processed_count} из {self.max_tasks}."
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
        topic_name: str,
        concurrency: int,
        rate_limit: dict,
        processor_callback,
        priorities: int = 1,
        max_retries: int = 3,
    ):
        self.topic_name = topic_name
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.processor_callback = processor_callback
        self.priorities = priorities
        self.nc = None
        self.js = None
        self.max_retries = max_retries
        self.active_tasks = 0

    async def connect(self):
        logger.info("Подключение воркера к NATS...")
        try:
            self.nc = await nats.connect(
                servers=["nats://localhost:4222"], user=user, password=password
            )
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

    async def send_to_dead_letter_queue(self, msg):
        try:
            await self.js.publish(f"{self.topic_name}.deadletter", msg.data)
            logger.info(f"Сообщение {msg.subject} отправлено в DLQ.")
        except Exception as e:
            logger.error(f"Ошибка при отправке в DLQ: {e}")

    async def _process_task(self, msg):
        try:
            self.active_tasks += 1
            job_data = json.loads(msg.data.decode())
            job_start_time = datetime.fromisoformat(job_data["meta"]["start_time"])
            if job_start_time > datetime.now():
                planned_time = job_start_time - datetime.now()
                delay = int(planned_time.total_seconds())
                logger.info(
                    f"Время для задачи {job_data['name']} еще не наступило. Повторная отправка через {delay}."
                )
                self.active_tasks -= 1
                await msg.nak(delay=delay)
                logger.info(f"Задача {job_data['name']} была повторно отправленна")
                return

            logger.info(f"Обработка задачи {job_data['name']}")

            if job_data.get("meta").get("retry_count") > self.max_retries:
                raise ValueError(
                    f"Максимальное количество попыток для задачи {job_data['name']} превышено."
                )

            timeout = job_data["meta"]["timeout"]
            await asyncio.wait_for(self.processor_callback(job_data), timeout=timeout)

            await msg.ack()
            logger.info(f"Задача {job_data['name']} успешно обработана.")
        except ValueError as e:
            logger.error(e)
            await msg.term()
        except asyncio.TimeoutError as e:
            logger.error(
                f"Ошибка при обработке задачи {job_data['name']} истек timeout: {e}",
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
        except Exception as e:
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

    async def fetch_messages(self, sub):
        try:
            msgs = await sub.fetch(self.concurrency, timeout=10)
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

        limiter = RateLimiter(self.rate_limit["max"], self.rate_limit["duration"])

        while True:
            messages_fetched = False

            for sub in subscriptions:
                msgs = await self.fetch_messages(sub)

                if msgs:
                    messages_fetched = True

                if messages_fetched:
                    break
                else:
                    logger.info("Нет сообщений для обработки, идем в следующий поток")

            if messages_fetched:
                while msgs:
                    await limiter.check_limit()
                    if self.active_tasks == self.concurrency:
                        logger.info("Concurrency достиг лимита, ожидаем...")
                        await asyncio.sleep(10)
                        continue
                    job = msgs.pop(0)
                    asyncio.create_task(self._process_task(job))
                    limiter.increment()
                else:
                    logger.info(
                        "Нет сообщений для обработки в данном потоке смотрим опять"
                    )


async def process_job(job_data):
    # logger.info(f"Выполняется {job_data['name']} что-то делает...")
    await asyncio.sleep(6)


async def main():
    try:
        queue = Queue(topic_name="my_queue", priorities=3)
        await queue.connect()

        jobs1 = [
            Job(
                queue_name="my_queue",
                name=f"task_{i}",
                data={"key": f"value_{i}"},
                timeout=10,
            )
            for i in range(1, 8)
        ]
        jobs2 = [
            Job(
                queue_name="my_queue",
                name=f"task_{i}",
                data={"key": f"value_{i}"},
                timeout=10,
            )
            for i in range(8, 13)
        ]

        await queue.addJobs(jobs2, 2)
        await queue.addJobs(jobs1, 1)

        worker = Worker(
            topic_name="my_queue",
            concurrency=3,
            rate_limit={"max": 5, "duration": 15000},
            processor_callback=process_job,
            priorities=3,
            max_retries=1,
        )

        await worker.connect()

        await worker.start()
    except Exception as e:
        logger.error(f"Ошибка в main: {e}", exc_info=True)
    finally:
        logger.info("Остановка всех воркеров и очистка стрима...")
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
