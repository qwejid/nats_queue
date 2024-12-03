import pytest
import pytest_asyncio
import json
from nats_queue.nats_queue import Queue
from nats_queue.nats_job import Job
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext as JetStream
import nats


@pytest_asyncio.fixture
async def get_nc():
    nc = await nats.connect()
    yield nc

    js = nc.jetstream()
    streams = await js.streams_info()
    stream_names = [stream.config.name for stream in streams]

    for name in stream_names:
        await js.delete_stream(name)

    await nc.close()


@pytest.mark.asyncio
async def test_queue_initialization(get_nc):
    topic_name = "test_topic"
    priorities = 3
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name, priorities=priorities)
    assert queue.topic_name == topic_name
    assert queue.priorities == priorities
    assert isinstance(queue.nc, NATS)
    assert queue.js is None


@pytest.mark.asyncio
async def test_queue_connect_success(get_nc):
    topic_name = "test_topic"
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name)

    await queue.connect()
    assert isinstance(queue.js, JetStream)


@pytest.mark.asyncio
async def test_queue_close_success():
    topic_name = "test_topic"
    nc = await nats.connect(servers=["nats://localhost:4222"])
    queue = Queue(nc, topic_name=topic_name)

    await queue.connect()
    await queue.js.delete_stream(queue.topic_name)
    await queue.close()
    assert queue.nc.is_closed


@pytest.mark.asyncio
async def test_add_job_success(get_nc):
    topic_name = "test_queue"
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name, priorities=3)
    await queue.connect()

    job = Job(queue_name="test_queue", name="test_job", data={"key": "value"})
    await queue.addJob(job, priority=1)

    sub = await queue.js.subscribe(f"{job.queue_name}.{job.name}.*")
    message = await sub.next_msg()

    assert message is not None
    assert json.loads(message.data.decode()) == job.to_dict()


@pytest.mark.asyncio
async def test_add_jobs_success(get_nc):
    topic_name = "test_queue"
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name, priorities=3)
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


@pytest.mark.asyncio
async def test_add_job_with_priority_success(get_nc):
    topic_name = "test_queue"
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name, priorities=3)
    await queue.connect()

    job_high = Job(
        queue_name="test_queue",
        name="high_priority_job",
        data={"key": "high_value"},
    )
    job_low = Job(
        queue_name="test_queue",
        name="low_priority_job",
        data={"key": "low_value"},
    )

    await queue.addJob(job_high, priority=1)
    await queue.addJob(job_low, priority=3)

    # Проверка высокого приоритета
    sub_high = await queue.js.subscribe(f"{job_high.queue_name}.{job_high.name}.1")
    message_high = await sub_high.next_msg()
    assert message_high is not None
    assert json.loads(message_high.data.decode()) == job_high.to_dict()

    # Проверка низкого приоритета
    sub_low = await queue.js.subscribe(f"{job_low.queue_name}.{job_low.name}.3")
    message_low = await sub_low.next_msg()
    assert message_low is not None
    assert json.loads(message_low.data.decode()) == job_low.to_dict()


@pytest.mark.asyncio
async def test_add_job_in_non_existent_stream(get_nc):
    topic_name = "non_existent_stream"
    nc = get_nc
    queue = Queue(nc, topic_name=topic_name)

    await queue.connect()

    job = Job(queue_name="test_queue", name="test_job", data={"key": "value"})

    with pytest.raises(Exception):
        await queue.addJob(job, priority=1)


@pytest.mark.asyncio
async def test_connect_raises_exception():
    queue = Queue("example_con", topic_name="test_topic")

    with pytest.raises(Exception):
        await queue.connect()


@pytest.mark.asyncio
async def test_create_stream_with_diff_conf(get_nc):
    nc = get_nc

    queue = Queue(nc, topic_name="my_queue", duplicate_window=1)
    await queue.connect()

    stream = await queue.js.stream_info(queue.topic_name)
    name = stream.config.name
    duplicate_window = stream.config.duplicate_window
    assert name == "my_queue"
    assert duplicate_window == queue.duplicate_window

    queue2 = Queue(nc, topic_name="my_queue", duplicate_window=3)
    await queue2.connect()

    stream = await queue.js.stream_info(queue.topic_name)
    name = stream.config.name
    duplicate_window = stream.config.duplicate_window
    assert name == "my_queue"
    assert duplicate_window == queue2.duplicate_window
