from datetime import datetime, timedelta
import logging
import uuid

logger = logging.getLogger("nats")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Job:
    def __init__(self, queue_name, name, data, timeout=None, delay=0, meta=None):

        if not queue_name:
            raise ValueError("Parameter 'queue_name' cannot be empty")
        if not name:
            raise ValueError("Parameter 'name' cannot be empty")

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

        logger.info(
            f"Created job with ID={self.id}, queue_name={self.queue_name}, "
            f"name={self.name}, delay={self.delay}ms, timeout={self.timeout}"
        )

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
