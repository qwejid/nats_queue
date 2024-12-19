import logging
from nats.aio.client import Client
from nats.js.errors import BadRequestError
import json
from dotenv import load_dotenv

from nats_queue.nats_job import Job

load_dotenv()

logger = logging.getLogger("nats")
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)


class Queue:
    def __init__(
        self,
        connection: Client,
        topic_name: str,
        priorities: int = 1,
        duplicate_window: int = 2000,
    ):
        if not topic_name:
            raise ValueError("Parameter 'topic_name' cannot be empty.")
        if priorities <= 0:
            raise ValueError("Parameter 'priorities' must be greater than 0.")

        self.topic_name = topic_name
        self.priorities = priorities
        self.nc = connection
        self.js = None
        self.duplicate_window = duplicate_window

        logger.info(
            f"Queue initialized with topic_name={self.topic_name}, "
            f"priorities={self.priorities}, duplicate_window={self.duplicate_window}ms"
        )

    async def connect(self):
        logger.info("Connecting to JetStream...")
        try:
            self.js = self.nc.jetstream()
            logger.info("Successfully connected to JetStream")

            subjects = [f"{self.topic_name}.*.*"]
            logger.debug(f"Creating stream with subjects: {subjects}")
            await self.js.add_stream(
                name=self.topic_name,
                subjects=subjects,
                duplicate_window=self.duplicate_window,
            )
            logger.info(f"Stream '{self.topic_name}' created successfully.")
        except BadRequestError:
            logger.warning(
                f"Stream '{self.topic_name}' already exists. Attempting to update..."
            )
            await self.js.update_stream(
                name=self.topic_name, duplicate_window=self.duplicate_window
            )
            logger.info(f"Stream '{self.topic_name}' updated successfully.")
        except Exception as e:
            logger.error(f"Error connecting to JetStream: {e}", exc_info=True)
            raise

    async def close(self):
        if self.nc:
            logger.info("Closing connection to NATS...")
            await self.nc.close()
            logger.info("Connection to NATS closed.")

    async def addJob(self, job: Job, priority: int = 1):
        if not isinstance(job, Job):
            raise ValueError("Parameter 'job' must be an instance of Job.")

        if priority >= self.priorities:
            priority = self.priorities
        elif priority == 0:
            priority = 1
        logger.info(
            f"Adding job ID={job.id} to queue='{job.queue_name}', "
            f"subject='{job.subject}' with priority={priority}"
        )
        try:
            job_data = json.dumps(job.to_dict()).encode()
            await self.js.publish(
                f"{job.queue_name}.{job.name}.{priority}",
                job_data,
                headers={"Nats-Msg-Id": job.id},
            )
            logger.info(f"Job ID={job.id} added successfully.")
        except Exception as e:
            logger.error(f"Failed to add job ID={job.id}: {e}", exc_info=True)
            raise

    async def addJobs(self, jobs: list[Job], priority: int = 1):
        if not all(isinstance(job, Job) for job in jobs):
            raise ValueError("All items in 'jobs' must be instances of Job.")

        logger.info(
            f"Adding {len(jobs)} jobs to queue='{self.topic_name}' "
            f"with priority={priority}"
        )
        for job in jobs:
            await self.addJob(job, priority)
        logger.info(f"All {len(jobs)} jobs added successfully.")
