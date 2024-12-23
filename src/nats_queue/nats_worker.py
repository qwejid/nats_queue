import asyncio
from datetime import datetime
import json
import logging
import time
from typing import Awaitable, Callable, Dict, List, Optional
import uuid
from logging import Logger
from nats_queue.nats_limiter import FixedWindowLimiter, IntervalLimiter
from nats.js.client import JetStreamContext
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats_queue.nats_job import Job
from nats.errors import TimeoutError

logger = logging.getLogger("nats_queue")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Worker:

    def __init__(
        self,
        client: Client,
        name: str,
        processor: Callable[[Dict], Awaitable[None]],
        concurrency: int = 1,
        max_retries: int = 3,
        priorities: int = 1,
        limiter: Dict[str, int] = None,
        logger: Logger = logger,
    ):
        self.client = client
        self.name = name
        self.processor = processor
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.priorities = priorities
        self.fetch_interval = 0.15
        self.fetch_timeout = 3
        self.limiter = (
            FixedWindowLimiter(
                limiter.get("max"),
                limiter.get("duration"),
                self.fetch_interval,
            )
            if limiter
            else IntervalLimiter(self.fetch_interval)
        )

        self.manager: Optional[JetStreamContext] = None
        self.consumers: Optional[List[JetStreamContext.PullSubscription]] = None
        self.running: bool = False
        self.processing_now: int = 0
        self.loop_task: Optional[asyncio.Task] = None
        self.logger: Logger = logger

        self.logger.info(
            (
                f"Worker initialized with name={self.name}, "
                f"concurrency={self.concurrency}, "
                f"priorities={self.priorities}"
            )
        )

    async def setup(self):
        try:
            self.manager = self.client.jetstream()
            self.consumers = await self.get_subscriptions()
        except Exception as e:
            raise e

    async def stop(self):
        self.running = False
        if self.loop_task:
            await self.loop_task
        while self.processing_now > 0:
            await asyncio.sleep(self.fetch_interval)

    async def start(self):
        if not self.consumers:
            raise Exception("Call setup() before start()")

        if not self.loop_task:
            self.running = True
            self.loop_task = asyncio.create_task(self.loop())

    async def loop(self):
        while self.running:
            for consumer in self.consumers:
                max_jobs = self.limiter.get(self.concurrency - self.processing_now)
                if max_jobs == 0:
                    continue
                jobs = await self.fetch_messages(consumer, max_jobs)
                if jobs:
                    break

            for job in jobs:
                self.limiter.inc()
                asyncio.create_task(self._process_task(job))

            await asyncio.sleep(self.limiter.timeout())

    async def _process_task(self, job: Msg):
        try:
            self.processing_now += 1
            job_data = json.loads(job.data.decode())
            job_start_time = datetime.fromisoformat(job_data["meta"]["start_time"])
            if job_start_time > datetime.now():
                planned_time = job_start_time - datetime.now()
                delay = int(planned_time.total_seconds())
                await job.nak(delay=delay)
                self.logger.debug(
                    (
                        f"Job:{job_data['name']} id={job_data['id']} is scheduled later "
                        f"Requeueing in {delay} seconds"
                    )
                )
                return

            if job_data.get("meta").get("retry_count") > self.max_retries:
                await job.term()
                self.logger.warning(
                    f"Job: {job_data['name']} id={job_data['id']} max retries exceeded"
                )
                return

            self.logger.info(
                (
                    f"ob: {job_data['name']} id={job_data['id']} is started "
                    f"with data={job_data['data']}) in queue={job_data['queue_name']}"
                )
            )

            timeout = job_data["meta"]["timeout"]
            await asyncio.wait_for(self.processor(job_data["data"]), timeout=timeout)

            await job.ack_sync()
            self.logger.info(
                f'Job: {job_data["name"]} id={job_data["id"]} is completed'
            )

        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                self.logger.error(
                    f"Job: {job_data['name']} id={job_data['id']} TimeoutError: {e}"
                )
            else:
                self.logger.error(f"Error while processing job {job_data['id']}: {e}")

            job_data["meta"]["retry_count"] += 1
            new_job = Job(
                queue_name=job_data["queue_name"],
                name=job_data["name"],
                data=job_data["data"],
                meta=job_data["meta"],
            )
            job_bytes = json.dumps(new_job.to_dict()).encode()
            await job.term()
            await self.manager.publish(
                job.subject,
                job_bytes,
                headers={"Nats-Msg-Id": f"{uuid.uuid4()}_{int(time.time())}"},
            )
        finally:
            self.processing_now -= 1

    async def fetch_messages(
        self, sub: JetStreamContext.PullSubscription, count
    ) -> List[Optional[Msg]]:
        try:
            msgs = await sub.fetch(count, timeout=self.fetch_timeout)
            self.logger.debug(
                (
                    f"Consumer: name={(await sub.consumer_info()).name} "
                    f"fetched {len(msgs)} messages"
                    ""
                )
            )
            return msgs
        except TimeoutError:
            self.logger.debug(
                (
                    f"Consumer: name={(await sub.consumer_info()).name} "
                    f"failed to fetch messages: TimeoutError"
                )
            )
            return []
        except Exception as e:
            self.logger.error(
                (
                    f"Consumer: name={(await sub.consumer_info()).name} "
                    f"error while fetching messages: {e}"
                )
            )
            raise

    async def get_subscriptions(self) -> List[JetStreamContext.PullSubscription]:
        subscriptions = []
        for priority in range(1, self.priorities + 1):
            topic = f"{self.name}.*.{priority}"
            try:
                sub = await self.manager.pull_subscribe(
                    topic, durable=f"worker_group_{priority}"
                )
                self.logger.info(
                    (
                        f"Consumer: name={self.name} "
                        f"successfully subscribed to topic {topic}."
                    )
                )
                subscriptions.append(sub)
            except Exception as e:
                self.logger.error(
                    f"Consumer: name={self.name} error "
                    f"while subscribing to topic {topic}: {e}"
                )
                raise

        return subscriptions
