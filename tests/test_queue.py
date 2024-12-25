from typing import Dict, List, Union
import pytest
import pytest_asyncio
import json
from nats_queue.nats_queue import Queue
from nats_queue.nats_job import Job
from nats.aio.client import Client
from nats.js.client import JetStreamContext as JetStream
import nats


@pytest_asyncio.fixture
async def get_client():
    client: Client = await nats.connect()
    yield client

    manager = client.jetstream()
    streams = await manager.streams_info()
    stream_names = [stream.config.name for stream in streams]

    for name in stream_names:
        await manager.delete_stream(name)

    await client.close()


@pytest.mark.asyncio
async def test_queue_initialization(get_client):
    name = "test_topic"
    priorities = 3
    client: Client = get_client
    queue = Queue(client, name, priorities)
    assert queue.name == name
    assert queue.priorities == priorities
    assert isinstance(queue.client, Client)
    assert queue.manager is None


@pytest.mark.asyncio
async def test_queue_initialization_error(get_client):
    name = ""
    priorities = 3
    client = get_client
    with pytest.raises(ValueError, match="Parameter 'name' cannot be empty"):
        Queue(client, name, priorities)
    with pytest.raises(ValueError, match=""):
        Queue(client, "name", 0)


@pytest.mark.asyncio
async def test_queue_connect_success(get_client):
    name = "test_topic"
    client = get_client
    queue = Queue(client, name=name)

    await queue.setup()
    assert isinstance(queue.manager, JetStream)


@pytest.mark.asyncio
async def test_queue_close_success():
    name = "test_topic"
    client = await nats.connect(servers=["nats://localhost:4222"])
    queue = Queue(client, name=name)

    await queue.setup()
    await queue.manager.delete_stream(queue.name)
    await queue.close()
    assert queue.client.is_closed is True


@pytest.mark.asyncio
async def test_add_job_success(get_client):
    name = "test_queue"
    client = get_client
    queue = Queue(client, name, priorities=3)
    job = Job(
        queue_name="test_queue",
        name="test_job",
        data={"key": "value"},
    )

    await queue.setup()
    await queue.addJob(job, priority=1)
    message = await queue.manager.get_last_msg(
        stream_name=queue.name, subject=f"{job.queue_name}.{job.name}.*"
    )

    assert message is not None
    assert json.loads(message.data.decode()) == job.to_dict()


@pytest.mark.asyncio
async def test_add_jobs_success(get_client):
    name = "test_queue"
    client = get_client
    queue = Queue(client, name, 3)
    jobs = [
        Job(
            queue_name="test_queue",
            name="job1",
            data={"key": "value1"},
        ),
        Job(
            queue_name="test_queue",
            name="job2",
            data={"key": "value2"},
        ),
    ]

    await queue.setup()
    await queue.addJobs(jobs, 1)

    for job in jobs:
        message = await queue.manager.get_last_msg(
            stream_name=job.queue_name, subject=f"{job.queue_name}.{job.name}.*"
        )

        assert message is not None
        assert json.loads(message.data.decode()) == job.to_dict()


@pytest.mark.asyncio
async def test_add_job_with_priority_success(get_client):
    name = "test_queue"
    client = get_client
    queue = Queue(client, name=name, priorities=3)
    await queue.setup()

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
    sub_high = await queue.manager.subscribe(f"{job_high.queue_name}.{job_high.name}.1")
    message_high = await sub_high.next_msg()
    assert message_high is not None
    assert json.loads(message_high.data.decode()) == job_high.to_dict()

    # Проверка низкого приоритета
    sub_low = await queue.manager.subscribe(f"{job_low.queue_name}.{job_low.name}.3")
    message_low = await sub_low.next_msg()
    assert message_low is not None
    assert json.loads(message_low.data.decode()) == job_low.to_dict()


@pytest.mark.asyncio
async def test_add_job_in_non_existent_stream(get_client):
    name = "non_existent_stream"
    client = get_client
    queue = Queue(client, name)

    await queue.setup()

    job = Job(
        queue_name="test_queue",
        name="test_job",
        data={"key": "value"},
    )

    with pytest.raises(Exception):
        await queue.addJob(job, priority=1)


@pytest.mark.asyncio
async def test_connect_raises_exception():
    queue = Queue("example_con", name="test_topic")

    with pytest.raises(Exception):
        await queue.setup()


@pytest.mark.asyncio
async def test_create_stream_with_diff_conf(get_client):
    client = get_client

    queue = Queue(client, name="my_queue", duplicate_window=1)
    await queue.setup()

    stream = await queue.manager.stream_info(queue.name)
    name = stream.config.name
    duplicate_window = stream.config.duplicate_window
    assert name == "my_queue"
    assert duplicate_window == queue.duplicate_window

    queue2 = Queue(client, name="my_queue", duplicate_window=3)
    await queue2.setup()

    stream = await queue.manager.stream_info(queue.name)
    name = stream.config.name
    duplicate_window = stream.config.duplicate_window
    assert name == "my_queue"
    assert duplicate_window == queue2.duplicate_window


@pytest.mark.asyncio
async def test_close_one_client():
    client = await nats.connect(servers=["nats://localhost:4222"])
    queue = Queue(client, name="my_queue", duplicate_window=1)
    queue2 = Queue(client, name="my_queue2", duplicate_window=1)
    await queue.setup()
    await queue2.setup()

    await queue.close()
    assert queue.client.is_closed is True
    await queue2.close()
    assert queue2.client.is_closed is True


@pytest.mark.asyncio
async def test_add_job_to_close_client():
    client = await nats.connect(servers=["nats://localhost:4222"])
    queue = Queue(client, name="my_queue", duplicate_window=1)
    await queue.setup()
    await queue.close()
    assert queue.client.is_closed is True

    job = Job(
        queue_name="my_queue",
        name="test_job",
        data={"key": "value"},
    )
    with pytest.raises(
        Exception, match="Cannot add job when NATS connection is closed."
    ):
        await queue.addJob(job, priority=1)


@pytest.mark.asyncio
async def test_create_queue_with_one_conf(get_client):
    client = get_client
    queue = Queue(client, name="my_queue", duplicate_window=1)
    await queue.setup()

    queue2 = Queue(client, name="my_queue", duplicate_window=1)
    await queue2.setup()


@pytest.mark.asyncio
async def test_create_flow_job(get_client):

    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    flowJob: Dict[
        str, Union[Job, List[Dict[str, Union[Job, List[Dict[str, Job]]]]]]
    ] = {
        "job": Job("my_queue", "parent_job"),
        "children": [
            {
                "job": Job("my_queue", "child_job_1"),
                "children": [
                    {
                        "job": Job("my_queue", "child_job_1_1"),
                    },
                    {"job": Job("my_queue", "child_job_1_2")},
                ],
            },
            {"job": Job("my_queue", "child_job_2")},
        ],
    }
    parent_job_id = [flowJob["job"].id, flowJob["children"][0]["job"].id]

    await queue.addFlowJob(flowJob)
    key_value = await queue.manager.key_value(f"{queue.name}_parent_id")
    kv_keys = await key_value.keys()
    assert set(kv_keys) == set(parent_job_id)

    stream_info = await queue.manager.stream_info(queue.name)
    messages = stream_info.state.messages
    assert messages == 3
