import logging
from typing import Dict, List, Optional, Union
from nats.aio.client import Client
from nats.js.kv import KeyValue
from nats.js.errors import BadRequestError
from nats.errors import ConnectionClosedError
import json
from dotenv import load_dotenv
from logging import Logger
from nats_queue.nats_job import Job

load_dotenv()

logger = logging.getLogger("nats_queue")
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)

DEFAULT_DEDUPLICATE_WINDOW = 2000


class Queue:
    def __init__(
        self,
        client: Client,
        name: str,
        priorities: int = 1,
        duplicate_window: int = DEFAULT_DEDUPLICATE_WINDOW,
        logger: Logger = logger,
    ):
        if not name:
            raise ValueError("Parameter 'name' cannot be empty")
        if priorities <= 0:
            raise ValueError("Parameter 'priorities' must be greater than 0")

        self.name = name
        self.priorities = priorities
        self.client = client
        self.manager = None
        self.duplicate_window = duplicate_window
        self.kv: Optional[KeyValue] = None
        self.logger: Logger = logger

        self.logger.info(
            f"Queue initialized with name={self.name}, priorities={self.priorities}"
        )

    async def setup(self):
        try:
            self.manager = self.client.jetstream()

            subjects = [f"{self.name}.*.*"]
            await self.manager.add_stream(
                name=self.name,
                subjects=subjects,
                duplicate_window=self.duplicate_window,
            )
            self.logger.info(f"Stream '{self.name}' created successfully.")

            self.kv = await self.manager.create_key_value(
                bucket=f"{self.name}_parent_id"
            )

        except BadRequestError:
            self.logger.warning(
                f"Stream '{self.name}' already exists. Attempting to update"
            )
            await self.manager.update_stream(
                name=self.name,
                subjects=subjects,
                duplicate_window=self.duplicate_window,
            )
            self.logger.info(f"Stream '{self.name}' updated successfully.")
        except Exception as e:
            self.logger.error(f"Error connecting to JetStream: {e}", exc_info=True)
            raise

    async def close(self):
        try:
            if self.client:
                await self.client.close()
                self.logger.info("Connection to NATS closed.")
        except ConnectionClosedError:
            self.logger.warning("Connection to NATS already closed.")

    async def addJob(self, job: Job, priority: int = 1):
        if self.client.is_closed:
            raise Exception("Cannot add job when NATS connection is closed.")
        if self.manager is None:
            raise Exception("Call setup before creating a new job")
        if not isinstance(job, Job):
            raise ValueError("Parameter 'job' must be an instance of Job.")

        if priority >= self.priorities:
            priority = self.priorities
        elif priority <= 0:
            priority = 1

        try:
            job_data = json.dumps(job.to_dict()).encode()
            await self.manager.publish(
                f"{job.queue_name}.{job.name}.{priority}",
                job_data,
                headers={"Nats-Msg-Id": job.id},
            )
            self.logger.info(f"Job ID={job.id} added successfully.")
        except Exception as e:
            self.logger.error(f"Failed to add job ID={job.id}: {e}", exc_info=True)
            raise

    async def addJobs(self, jobs: list[Job], priority: int = 1):
        if not all(isinstance(job, Job) for job in jobs):
            raise ValueError("All items in 'jobs' must be instances of Job.")

        for job in jobs:
            await self.addJob(job, priority)

    async def addFlowJob(
        self, tree: Dict[str, Union[List[Job], Job]], priority: int = 1
    ):
        async def traverse(node: Dict[str, Union[List[Job], Job]], parent_id=None):
            current_job: Job = node["job"]
            if parent_id:
                current_job.meta["parent_id"] = parent_id

            children = node.get("children", [])
            if not children:
                return [current_job]

            await self.kv.put(
                current_job.id,
                json.dumps(
                    {**current_job.to_dict(), "children_count": len(children)}
                ).encode(),
            )

            deepest_jobs = []
            for child in children:
                deepest_jobs.extend(await traverse(child, current_job.id))

            return deepest_jobs

        deepest_jobs = await traverse(tree)
        await self.addJobs(deepest_jobs, priority)
