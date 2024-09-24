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

@pytest.mark.asyncio
async def test_add_multiple_jobs():
    topic_name = "test_queue"
    queue = Queue(topic_name=topic_name, priorities=3)
    await queue.connect()

    jobs = [
        Job(queue_name="test_queue", name="job1", data={"key": "value1"}),
        Job(queue_name="test_queue", name="job2", data={"key": "value2"}),
    ]
    await queue.addJobs(jobs)

    for job in jobs:
        sub = await queue.js.subscribe(f"{job.queue_name}.{job.name}.*")
        message = await sub.next_msg()
        assert message is not None
        assert json.loads(message.data.decode()) == job.to_dict()

    await queue.js.delete_stream(queue.topic_name)
    await queue.close()

@pytest.mark.asyncio
async def test_add_job_with_priority():
    topic_name = "test_queue"
    queue = Queue(topic_name=topic_name, priorities=3)
    await queue.connect()

    job_high = Job(queue_name="test_queue", name="high_priority_job", data={"key": "high_value"})
    job_low = Job(queue_name="test_queue", name="low_priority_job", data={"key": "low_value"})

    await queue.addJob(job_high, priority=0) 
    await queue.addJob(job_low, priority=2)    

    # Проверка высокого приоритета
    sub_high = await queue.js.subscribe(f"{job_high.queue_name}.{job_high.name}.*")
    message_high = await sub_high.next_msg()
    assert message_high is not None
    assert json.loads(message_high.data.decode()) == job_high.to_dict()

    # Проверка низкого приоритета
    sub_low = await queue.js.subscribe(f"{job_low.queue_name}.{job_low.name}.*")
    message_low = await sub_low.next_msg()
    assert message_low is not None
    assert json.loads(message_low.data.decode()) == job_low.to_dict()

    await queue.js.delete_stream(queue.topic_name)
    await queue.close()

@pytest.mark.asyncio
async def test_no_stream():
    topic_name = "non_existent_stream"
    queue = Queue(topic_name=topic_name)
    
    await queue.connect()

    job = Job(queue_name="test_queue", name="test_job", data={"key": "value"})
    
    with pytest.raises(Exception):
        await queue.addJob(job, priority=1)

    await queue.js.delete_stream(queue.topic_name)
    await queue.close()

@pytest.mark.asyncio
async def test_close_connection():
    topic_name = "test_queue"
    queue = Queue(topic_name=topic_name)
    
    await queue.connect()
    await queue.js.delete_stream(queue.topic_name)
    await queue.close()
    
    assert queue.nc.is_closed


