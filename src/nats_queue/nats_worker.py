import asyncio
from datetime import datetime
import json
import logging
from nats_queue.nats_limiter import RateLimiter
from nats.aio.client import Client
from nats_queue.nats_job import Job
from nats.errors import TimeoutError

logger = logging.getLogger("nats")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Worker:
    def __init__(
        self,
        connection: Client,
        topic_name: str,
        processor_callback,
        rate_limit: tuple,
        concurrency: int = 1,
        timeout_fetch: int = 5,
        interval_fetch: float = 0.15,
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
        self.interval_fetch = interval_fetch

        logger.info(
            f"Worker initialized with topic_name={self.topic_name}, "
            f"concurrency={self.concurrency}, priorities={self.priorities}ms"
        )

    async def connect(self):
        logger.info("Connecting worker to NATS...")
        try:
            self.js = self.nc.jetstream()
            logger.info("Worker successfully connected to NATS.")
        except Exception as e:
            logger.error(f"Error while connecting worker to NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Closing worker...")
            await self.nc.close()
            logger.info("Worker successfully disconnected.")

    async def _process_task(self, msg):
        try:
            self.active_tasks += 1
            job_data = json.loads(msg.data.decode())
            job_start_time = datetime.fromisoformat(job_data["meta"]["start_time"])
            if job_start_time > datetime.now():
                planned_time = job_start_time - datetime.now()
                delay = int(planned_time.total_seconds())
                logger.info(
                    f"Job {job_data['name']} is scheduled for later. "
                    f"Requeueing in {delay} seconds."
                )
                await msg.nak(delay=delay)
                logger.info(f"Job {job_data['name']} requeued successfully.")
                return

            logger.info(f"Processing job {job_data['name']}...")

            if job_data.get("meta").get("retry_count") > self.max_retries:
                logger.error(f"Max retries exceeded for job {job_data['name']}.")
                await msg.term()
                return

            timeout = job_data["meta"]["timeout"]
            await asyncio.wait_for(
                self.processor_callback(job_data["data"]), timeout=timeout
            )

            await msg.ack_sync()
            logger.info(f"Job {job_data['name']} processed successfully.")

        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                logger.error(
                    f"Timeout error while processing job {job_data['name']}: {e}",
                    exc_info=True,
                )
            else:
                logger.error(
                    f"Error while processing job {job_data['name']}: {e}",
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
            logger.info(f"Fetched {len(msgs)} messages.")
            return msgs
        except TimeoutError:
            logger.debug("Failed to fetch messages: Timeout occurred.")
        except Exception as e:
            logger.error(f"Error while fetching messages: {e}")
            raise

    async def get_subscriptions(self):
        subscriptions = []
        for priority in range(1, self.priorities + 1):
            topic = f"{self.topic_name}.*.{priority}"
            try:
                sub = await self.js.pull_subscribe(
                    topic, durable=f"worker_group_{priority}"
                )
                logger.info(f"Successfully subscribed to topic {topic}.")
                subscriptions.append(sub)

            except Exception as e:
                logger.error(f"Error while subscribing to topic {topic}: {e}")
                raise
        return subscriptions

    async def start(self):
        subscriptions = await self.get_subscriptions()

        limiter = RateLimiter(
            self.rate_limit[0],
            self.rate_limit[1],
            self.concurrency,
            self.interval_fetch,
        )

        while True:
            messages_fetched = False

            for sub in subscriptions:
                free_slot = await limiter.check_limit()
                fetch_count = free_slot if free_slot else self.concurrency

                msgs = await self.fetch_messages(sub, fetch_count)

                if msgs:
                    messages_fetched = True

                if messages_fetched:
                    break
                else:
                    logger.debug("No messages available, proceeding to the next loop.")

            if messages_fetched:

                tasks = [self._process_task(job) for job in msgs]
                asyncio.gather(*tasks)
                limiter.limiter.inc(len(tasks))
