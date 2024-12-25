import json
from typing import Dict, List, Union
import pytest
import asyncio
import pytest_asyncio
from nats_queue.nats_limiter import FixedWindowLimiter
from nats_queue.nats_queue import Queue
from nats_queue.nats_worker import Worker
from nats_queue.nats_job import Job
from nats.aio.client import Client
from nats.js.client import JetStreamContext
import nats
from nats.js.errors import NoKeysError


@pytest_asyncio.fixture
async def get_client():
    connect: Client = await nats.connect()
    yield connect

    manager = connect.jetstream()
    streams = await manager.streams_info()
    stream_names = [stream.config.name for stream in streams]

    for name in stream_names:
        await manager.delete_stream(name)

    await connect.close()


@pytest_asyncio.fixture
async def job_delay():
    job = Job(
        queue_name="my_queue",
        name="task_1",
        data={"key": "value"},
        delay=5,
    )
    return job


@pytest.mark.asyncio
async def test_worker_initialization(get_client):
    client = get_client

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
        concurrency=3,
        limiter={"max": 5, "duration": 15},
    )

    assert isinstance(worker.client, Client)
    assert worker.name == "my_queue"
    assert worker.concurrency == 3
    assert worker.max_retries == 3
    assert isinstance(worker.limiter, FixedWindowLimiter)
    assert worker.priorities == 1
    assert worker.fetch_interval == 0.15
    assert worker.fetch_timeout == 3
    assert worker.running is False
    assert worker.processing_now == 0
    assert worker.loop_task is None
    assert worker.consumers is None
    assert worker.kv is None


@pytest.mark.asyncio
async def test_worker_setup_success(get_client):
    client: Client = get_client
    queue = Queue(client, "my_queue")
    await queue.setup()

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
    )

    await worker.setup()
    assert isinstance(worker.client, Client)
    assert isinstance(worker.manager, JetStreamContext)


@pytest.mark.asyncio
async def test_worker_setup_faild(get_client):
    client: Client = get_client
    queue = Queue(client, "my_queue")
    await queue.setup()

    worker = Worker(
        "client",
        name="my_queue",
        processor=process_job,
    )
    with pytest.raises(Exception):
        await worker.setup()


@pytest.mark.asyncio
async def test_worker_setup_unknow_queue(get_client):
    client: Client = get_client
    queue = Queue(client, "my_queue")
    await queue.setup()

    worker = Worker(
        client,
        name="my_queue_1",
        processor=process_job,
    )
    with pytest.raises(Exception):
        await worker.setup()


@pytest.mark.asyncio
async def test_worker_connect_stop_success(get_client):
    client: Client = get_client
    queue = Queue(client, "my_queue")
    await queue.setup()

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
    )

    await worker.setup()
    await worker.stop()
    assert worker.running is False


@pytest.mark.asyncio
async def test_worker_fetch_messages_success(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    jobs = [
        Job(
            queue_name="my_queue",
            name="task_1",
        ),
        Job(
            queue_name="my_queue",
            name="task_2",
        ),
    ]
    await queue.addJobs(jobs)

    worker = Worker(
        client,
        name="my_queue",
        concurrency=2,
        processor=process_job,
    )
    await worker.setup()

    consumers = await worker.manager.pull_subscribe(f"{worker.name}.*.*")

    jobs = await worker.fetch_messages(consumers, worker.concurrency)
    assert len(jobs) == worker.concurrency
    fetched_job_data_1 = json.loads(jobs[0].data.decode())
    assert fetched_job_data_1["name"] == "task_1"
    fetched_job_data_2 = json.loads(jobs[1].data.decode())
    assert fetched_job_data_2["name"] == "task_2"


@pytest.mark.asyncio
async def test_worker_fetch_messages_error(get_client):
    client = get_client

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
    )

    with pytest.raises(Exception):
        await worker.fetch_messages(None, worker.concurrency)


@pytest.mark.asyncio
async def test_worker_process_task_success(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    job_data = {"key": "value"}
    job = Job(
        queue_name="my_queue",
        name="task_1",
        data=job_data,
    )
    await queue.addJob(job)

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
    )
    await worker.setup()
    sub = await worker.manager.pull_subscribe(f"{worker.name}.*.*")
    msg = (await worker.fetch_messages(sub, worker.concurrency))[0]
    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"

    await worker._process_task(msg)
    msgs = await worker.fetch_messages(sub, worker.concurrency)
    assert msgs == []


@pytest.mark.asyncio
async def test_worker_process_task_with_retry(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(
        queue_name="my_queue",
        name="task_1",
        data={"key": "value"},
        timeout=1,
    )
    await queue.addJob(job)

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job_with_timeout,
    )
    await worker.setup()
    sub = await worker.get_subscriptions()

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1
    msg = msgs[0]
    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 0
    await worker._process_task(msg)

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1
    msg = msgs[0]
    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 1


@pytest.mark.asyncio
async def test_worker_process_task_exceeds_max_retries(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=5)
    job.meta["retry_count"] = 4
    await queue.addJob(job)

    worker = Worker(
        client,
        name="my_queue",
        limiter={"max": 5, "duration": 10},
        processor=process_job,
    )
    await worker.setup()
    sub = await worker.get_subscriptions()

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1
    msg = msgs[0]
    await worker._process_task(msg)
    job_data = json.loads(msg.data.decode())
    assert job_data["meta"]["retry_count"] == 4
    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert msgs == []


@pytest.mark.asyncio
async def test_worker_process_task_with_timeout(get_client):
    client = get_client

    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=1)
    await queue.addJob(job)

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job_with_timeout,
    )
    await worker.setup()
    sub = await worker.get_subscriptions()

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1
    msg = msgs[0]
    job_data = json.loads(msg.data.decode())

    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 0

    await worker._process_task(msg)
    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1
    msg = msgs[0]

    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 1


@pytest.mark.asyncio
async def test_worker_get_subscriptions(get_client):
    client = get_client
    queue = Queue(client, name="my_queue", priorities=3)
    await queue.setup()

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
        priorities=queue.priorities,
    )

    await worker.setup()
    subscriptions = await worker.get_subscriptions()
    worker_name = []
    durable_name = []
    for sub in subscriptions:
        info = await sub.consumer_info()
        filter_subject = info.config.filter_subject
        worker_name.append(filter_subject)
        durable = info.config.durable_name
        durable_name.append(durable)
    assert worker_name == ["my_queue.*.1", "my_queue.*.2", "my_queue.*.3"]
    assert durable_name == ["worker_group_1", "worker_group_2", "worker_group_3"]


@pytest.mark.asyncio
async def test_worker_get_subscriptions_error(get_client):
    client = get_client

    worker = Worker(client, name="my_queue", processor=process_job)
    with pytest.raises(Exception):
        await worker.get_subscriptions()


@pytest.mark.asyncio
async def test_worker_fetch_retry(get_client):
    client = get_client
    queue = Queue(client, name="my_queue", priorities=3)
    await queue.setup()

    jobs = [
        Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"})
        for i in range(1, 6)
    ]
    await queue.addJobs(jobs, 1)

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
        priorities=queue.priorities,
    )
    await worker.setup()

    sub = await worker.get_subscriptions()

    info = await sub[0].consumer_info()
    filter_subject = info.config.filter_subject
    assert filter_subject == "my_queue.*.1"

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    messages_worker1_len = len(msgs)
    assert messages_worker1_len == 3

    task_name = [msg.subject for msg in msgs]
    assert task_name == [
        "my_queue.task_1.1",
        "my_queue.task_2.1",
        "my_queue.task_3.1",
    ]
    task_ack = [msg.ack() for msg in msgs]
    asyncio.gather(*task_ack)

    info = await sub[0].consumer_info()
    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    messages_worker_len = len(msgs)
    assert messages_worker_len == 2

    task_name = [msg.subject for msg in msgs]
    assert task_name == ["my_queue.task_4.1", "my_queue.task_5.1"]

    task_ack = [msg.ack() for msg in msgs]
    asyncio.gather(*task_ack)

    messages_len = await worker.fetch_messages(sub[0], worker.concurrency)
    assert messages_len == []


@pytest.mark.asyncio
async def test_two_worker_fetch(get_client):
    nc = get_client
    queue = Queue(nc, name="my_queue", priorities=3)
    await queue.setup()

    jobs = [
        Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"})
        for i in range(1, 6)
    ]
    await queue.addJobs(jobs, 1)

    worker = Worker(
        nc,
        name="my_queue",
        concurrency=3,
        processor=process_job,
        priorities=queue.priorities,
    )

    worker2 = Worker(
        nc,
        name="my_queue",
        concurrency=4,
        processor=process_job,
        priorities=queue.priorities,
    )

    await worker.setup()
    await worker2.setup()

    sub = await worker.get_subscriptions()
    sub2 = await worker2.get_subscriptions()

    info1 = await sub[0].consumer_info()

    filter_subject1 = info1.config.filter_subject
    assert filter_subject1 == "my_queue.*.1"

    msgs1 = await worker.fetch_messages(sub[0], worker.concurrency)

    messages_worker1_len = len(msgs1)
    assert messages_worker1_len == 3

    task_name1 = [msg.subject for msg in msgs1]
    assert task_name1 == [
        "my_queue.task_1.1",
        "my_queue.task_2.1",
        "my_queue.task_3.1",
    ]
    task_ack1 = [msg.ack_sync() for msg in msgs1]

    info2 = await sub2[0].consumer_info()
    filter_subject2 = info2.config.filter_subject
    assert filter_subject2 == "my_queue.*.1"

    msgs2 = await worker2.fetch_messages(sub2[0], worker.concurrency)
    messages_worker2_len = len(msgs2)
    assert messages_worker2_len == 2

    task_name2 = [msg.subject for msg in msgs2]
    assert task_name2 == ["my_queue.task_4.1", "my_queue.task_5.1"]

    task_ack2 = [msg.ack_sync() for msg in msgs2]

    asyncio.gather(*task_ack1)
    asyncio.gather(*task_ack2)

    messages_len = await worker.fetch_messages(sub[0], worker.concurrency)
    assert messages_len == []

    messages_len2 = await worker2.fetch_messages(sub2[0], worker.concurrency)
    assert messages_len2 == []


@pytest.mark.asyncio
async def test_worker_planned_time(get_client, job_delay):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
    )

    await worker.setup()
    sub = await worker.get_subscriptions()

    await queue.addJob(job_delay)

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1

    msg = msgs[0]
    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 0

    await worker._process_task(msg)
    assert worker.processing_now == 0
    await asyncio.sleep(5)

    msgs = await worker.fetch_messages(sub[0], worker.concurrency)
    assert len(msgs) == 1

    msg = msgs[0]
    job_data = json.loads(msg.data.decode())
    assert job_data["name"] == "task_1"
    assert job_data["meta"]["retry_count"] == 0
    assert msg.metadata.num_delivered == 2


@pytest.mark.asyncio
async def test_worker_start_one_worker(get_client):
    client = get_client

    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=2)
    await queue.addJob(job)

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
    )
    await worker.setup()

    await worker.start()
    await asyncio.sleep(2)
    await worker.stop()

    stream_info = await worker.manager.streams_info()
    assert len(stream_info) == 2
    stream_name = [stream.config.name for stream in stream_info]
    assert set(stream_name) == set(["my_queue", f"KV_{queue.name}_parent_id"])
    assert stream_info[1].config.subjects == ["my_queue.*.*"]
    stream = stream_info[1].config.name
    assert stream == queue.name

    worker_count = stream_info[1].state.consumer_count
    assert worker_count == 1

    worker_info = await worker.manager.consumers_info(stream)
    assert len(worker_info) == 1

    worker_name = worker_info[0].name
    assert worker_name == "worker_group_1"


@pytest.mark.asyncio
async def test_worker_start_many_worker_with_one_durable(get_client):
    client = get_client

    queue = Queue(client, name="my_queue")
    queue2 = Queue(client, name="my_queue_2")
    await queue.setup()
    await queue2.setup()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=2)
    job2 = Job(queue_name="my_queue_2", name="task_1", data={"key": "value"}, timeout=2)
    await queue.addJob(job)
    await queue2.addJob(job2)

    worker = Worker(
        client,
        name="my_queue",
        processor=process_job,
        limiter={"max": 1, "duration": 10},
    )

    worker2 = Worker(
        client,
        name="my_queue_2",
        processor=process_job,
        limiter={"max": 1, "duration": 10},
    )
    await worker.setup()
    await worker2.setup()

    await worker.start()
    await worker2.start()

    await asyncio.sleep(2)

    await worker.stop()
    await worker2.stop()

    stream_info = await worker.manager.streams_info()
    assert len(stream_info) == 4
    stream_name = []
    for stream in stream_info:
        stream_name.append(stream.config.name)
    assert set(stream_name) == set(
        [
            "KV_my_queue_2_parent_id",
            "KV_my_queue_parent_id",
            "my_queue",
            "my_queue_2",
        ]
    )

    worker_count = 0
    for stream in stream_info:
        worker_count += stream.state.consumer_count
    assert worker_count == 2
    worker_info1 = await worker.manager.consumers_info(queue.name)
    assert len(worker_info1) == 1
    worker_name1 = worker_info1[0].name
    assert worker_name1 == "worker_group_1"

    worker2_info2 = await worker2.manager.consumers_info(queue2.name)
    assert len(worker2_info2) == 1

    worker_name2 = worker2_info2[0].name
    assert worker_name2 == "worker_group_1"

    assert worker_info1[0] != worker2_info2[0]


@pytest.mark.asyncio
async def test_worker_start(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=1)
    job2 = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=1)
    await queue.addJobs([job, job2])

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
    )
    await worker.setup()
    await worker.start()
    await asyncio.sleep(4)
    await worker.stop()
    stream_info = await worker.manager.streams_info()
    assert len(stream_info) == 2


@pytest.mark.asyncio
async def test_publish_parent_job(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    job = Job(
        queue_name="my_queue",
        name="parent_job",
        data={"key": "value"},
        timeout=1,
        meta={"parent_id": "1"},
    )

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
    )
    await worker.setup()
    await worker._publish_parent_job(job.to_dict())

    job_data = json.loads(
        (
            await worker.manager.get_last_msg(queue.name, f"{queue.name}.*.*")
        ).data.decode()
    )
    assert job.name == job_data["name"]


@pytest.mark.asyncio
async def test_mark_parents_failed(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()
    job_1 = Job(queue_name="my_queue", name="parent_job")
    job_2 = Job(queue_name="my_queue", name="child_job_1", meta={"parent_id": job_1.id})
    job_3 = Job(queue_name="my_queue", name="child_job_2", meta={"parent_id": job_2.id})

    await queue.kv.put(
        job_1.id,
        json.dumps({**job_1.to_dict(), "children_count": 1}).encode(),
    )
    await queue.kv.put(
        job_2.id,
        json.dumps({**job_2.to_dict(), "children_count": 1}).encode(),
    )

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
    )
    await worker.setup()

    await worker._mark_parents_failed(job_3.to_dict())

    sub = await worker.manager.pull_subscribe(f"{queue.name}.*.*")
    msg = await sub.fetch(5)
    job_data = [json.loads(job.data.decode()) for job in msg]

    assert job_data[0]["name"] == "child_job_1"
    assert job_data[0]["meta"]["failed"] is True

    assert job_data[1]["name"] == "parent_job"
    assert job_data[1]["meta"]["failed"] is True


@pytest.mark.asyncio
async def test_process_task_with_flow_job(get_client):
    client = get_client
    queue = Queue(client, name="my_queue")
    await queue.setup()

    parent_job = Job("my_queue", "parent_job")
    child_job_1 = Job("my_queue", "child_job_1")
    child_job_1_1 = Job("my_queue", "child_job_1_1")
    child_job_1_2 = Job("my_queue", "child_job_1_2")
    child_job_1_2_1 = Job("my_queue", "child_job_1_2_1")
    child_job_1_2_2 = Job("my_queue", "child_job_1_2_2")
    child_job_2 = Job("my_queue", "child_job_2")

    flowJob: Dict[
        str, Union[Job, List[Dict[str, Union[Job, List[Dict[str, Job]]]]]]
    ] = {
        "job": parent_job,
        "children": [
            {
                "job": child_job_1,
                "children": [
                    {"job": child_job_1_1},
                    {
                        "job": child_job_1_2,
                        "children": [
                            {"job": child_job_1_2_1},
                            {"job": child_job_1_2_2},
                        ],
                    },
                ],
            },
            {"job": child_job_2},
        ],
    }

    await queue.addFlowJob(flowJob)

    keys = await queue.kv.keys()

    assert set(keys) == set(
        [
            parent_job.id,
            child_job_1.id,
            child_job_1_2.id,
        ],
    )

    messages = (await queue.manager.stream_info(queue.name)).state.messages
    assert messages == 4

    worker = Worker(
        client,
        name="my_queue",
        concurrency=3,
        processor=process_job,
        limiter={"max": 2, "duration": 6},
    )

    await worker.setup()
    await worker.start()
    await asyncio.sleep(10)
    await worker.stop()

    with pytest.raises(NoKeysError):
        await queue.kv.keys()


async def process_job(job_data: Dict):
    await asyncio.sleep(1)


async def process_job_with_timeout(job_data: Dict):
    await asyncio.sleep(10)


async def process_job_with_error(job_data: Dict):
    raise Exception("Test Error")
