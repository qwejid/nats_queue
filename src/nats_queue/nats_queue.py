import logging
from nats.aio.client import Client
from nats.js.errors import BadRequestError
import json
from dotenv import load_dotenv

from nats_queue.nats_job import Job

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

logger = logging.getLogger("nats")


class Queue:
    def __init__(
        self,
        connection: Client,
        topic_name: str,
        priorities: int = 1,
        duplicate_window: int = 2000,
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
