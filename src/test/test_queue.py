import pytest
import json
from nats_queue.main import Queue, Job
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext as JetStream

@pytest.mark.asyncio
async def test_queue_initialization():
    topic_name = "test_topic"
    priorities = 3
    queue = Queue(topic_name=topic_name, priorities=priorities)
    assert queue.topic_name == topic_name
    assert queue.priorities == priorities
    assert queue.nc is None
    assert queue.js is None


@pytest.mark.asyncio
async def test_queue_connect_and_close():
    topic_name = "test_topic"
    queue = Queue(topic_name=topic_name)
    
    await queue.connect()
    assert isinstance(queue.nc, NATS)
    assert isinstance(queue.js, JetStream)
    await queue.js.delete_stream(queue.topic_name)
    await queue.close()
    assert queue.nc.is_closed

@pytest.mark.asyncio
async def test_add_job():
    topic_name = "test_queue"
    queue = Queue(topic_name=topic_name, priorities=3)
    await queue.connect()

    job = Job(queue_name="test_queue", name="test_job", data={"key": "value"})
    await queue.addJob(job, priority=1)

    sub = await queue.js.subscribe(f"{job.queue_name}.{job.name}.*")
    message = await sub.next_msg()

    assert message is not None
    assert json.loads(message.data.decode()) == job.to_dict()

    await queue.js.delete_stream(queue.topic_name)
    await queue.close()

