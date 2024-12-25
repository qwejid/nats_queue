from datetime import datetime, timedelta
import time
import uuid
from typing import Any, Dict


class Job:
    def __init__(
        self,
        queue_name: str,
        name: str,
        data: Dict[str, Any] = {},
        timeout=None,
        delay=0,
        meta={},
    ):
        for param, param_name in [
            (queue_name, "queue_name"),
            (name, "name"),
        ]:
            if not param:
                raise ValueError(f"Parameter '{param_name}' cannot be empty")

        self.id = f"{uuid.uuid4()}_{int(time.time())}"
        self.queue_name = queue_name
        self.name = name
        self.data = data
        self.meta = meta | {
            "retry_count": 0,
            "start_time": (datetime.now() + timedelta(seconds=delay)).isoformat(),
            "timeout": timeout,
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
