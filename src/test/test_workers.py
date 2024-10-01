import json
import pytest
import asyncio
from nats_queue.main import Worker, Job, Queue


@pytest.mark.asyncio
async def test_worker_initialization():
    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 15000},
        processor_callback=process_job,
    )

    assert worker.topic_name == "my_queue"
    assert worker.concurrency == 3
    assert worker.rate_limit == {"max": 5, "duration": 15000}
    assert worker.max_retries == 3


@pytest.mark.asyncio
async def test_worker_connect_and_close():
    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 30},
        processor_callback=process_job,
    )

    await worker.connect()
    assert worker.nc is not None
    await worker.close()
    assert worker.nc.is_closed


@pytest.mark.asyncio
async def test_worker_fetch_messages():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    jobs = [
        Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"})
        for i in range(1, 6)
    ]
    await queue.addJobs(jobs)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 15000},
        processor_callback=process_job,
    )
    await worker.connect()

    sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")

    try:

        msgs = await worker.fetch_messages(sub)

        assert len(msgs) == worker.concurrency
        fetched_job_data_1 = json.loads(msgs[0].data.decode())
        assert fetched_job_data_1["name"] == "task_1"
        fetched_job_data_2 = json.loads(msgs[1].data.decode())
        assert fetched_job_data_2["name"] == "task_2"

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_process_task_success():
    try:
        queue = Queue(topic_name="my_queue")
        await queue.connect()

        job_data = {
            "name": "task_1",
            "data": {"key": "value"},
            "retry_count": 0,
        }
        job = Job(queue_name="my_queue", name=job_data["name"], data=job_data["data"])
        await queue.addJob(job)

        worker = Worker(
            topic_name="my_queue",
            concurrency=3,
            rate_limit={"max": 5, "duration": 15000},
            processor_callback=process_job,
        )
        await worker.connect()

        sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")
        msg = (await worker.fetch_messages(sub))[0]

        await worker._process_task(msg)

        msgs = await worker.fetch_messages(sub)
        assert msgs is None

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_process_task_with_retry():

    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=20)
    await queue.addJob(job)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 15000},
        processor_callback=process_job_with_error,
    )
    await worker.connect()
    try:
        sub = await worker.get_subscriptions()
        msgs = await worker.fetch_messages(sub[0])
        assert len(msgs) == 1
        msg = msgs[0]

        await worker._process_task(msg)
        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 0

        msgs = await worker.fetch_messages(sub[0])
        assert len(msgs) == 1
        msg = msgs[0]

        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 1

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_process_task_exceeds_max_retries():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=20)
    job.meta["retry_count"] = 4
    await queue.addJob(job)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 15000},
        processor_callback=process_job,
    )
    await worker.connect()

    try:
        sub = await worker.get_subscriptions()
        msgs = await worker.fetch_messages(sub[0])

        assert len(msgs) == 1
        msg = msgs[0]

        await worker._process_task(msg)

        job_data = json.loads(msg.data.decode())

        assert job_data["meta"]["retry_count"] == 4

        msgs = await worker.fetch_messages(sub[0])
        assert msgs is None

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_process_task_with_timeout():

    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, timeout=1)
    await queue.addJob(job)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 2000},
        processor_callback=process_job_with_timeout,
    )
    await worker.connect()
    try:
        sub = await worker.get_subscriptions()
        msgs = await worker.fetch_messages(sub[0])

        assert len(msgs) == 1
        msg = msgs[0]

        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 0

        await worker._process_task(msg)

        msgs = await worker.fetch_messages(sub[0])
        assert len(msgs) == 1
        msg = msgs[0]
        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 1

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_get_subscriptions():
    queue = Queue(topic_name="my_queue", priorities=3)
    await queue.connect()

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 30},
        processor_callback=process_job,
        priorities=queue.priorities,
    )

    await worker.connect()
    try:
        subscriptions = await worker.get_subscriptions()

        worker_name = []
        for sub in subscriptions:
            info = await sub.consumer_info()
            filter_subject = info.config.filter_subject
            worker_name.append(filter_subject)

        assert worker_name == ["my_queue.*.1", "my_queue.*.2", "my_queue.*.3"]
    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


@pytest.mark.asyncio
async def test_worker_fetch_retry():
    queue = Queue(topic_name="my_queue", priorities=3)
    await queue.connect()

    jobs = [
        Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"})
        for i in range(1, 6)
    ]
    await queue.addJobs(jobs, 1)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 5000},
        processor_callback=process_job,
        priorities=queue.priorities,
    )

    worker2 = Worker(
        topic_name="my_queue",
        concurrency=4,
        rate_limit={"max": 5, "duration": 5000},
        processor_callback=process_job,
        priorities=queue.priorities,
    )

    await worker.connect()
    await worker2.connect()

    sub = await worker.get_subscriptions()
    sub2 = await worker2.get_subscriptions()

    try:
        info = await sub[0].consumer_info()
        filter_subject = info.config.filter_subject
        assert filter_subject == "my_queue.*.1"

        msgs = await worker.fetch_messages(sub[0])
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

        info = await sub2[0].consumer_info()
        filter_subject = info.config.filter_subject
        assert filter_subject == "my_queue.*.1"

        msgs = await worker2.fetch_messages(sub2[0])
        messages_worker2_len = len(msgs)
        assert messages_worker2_len == 2
        task_name = [msg.subject for msg in msgs]
        assert task_name == ["my_queue.task_4.1", "my_queue.task_5.1"]
        task_ack = [msg.ack() for msg in msgs]
        asyncio.gather(*task_ack)

        messages_len = await worker.fetch_messages(sub[0])
        assert messages_len is None

        messages_len2 = await worker2.fetch_messages(sub2[0])
        assert messages_len2 is None

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()
        await worker2.close()


@pytest.mark.asyncio
async def test_worker_planned_time():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job = Job(
        queue_name="my_queue",
        name="task_1",
        data={"key": "value"},
        delay=15000,
    )
    await queue.addJob(job)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 5000},
        processor_callback=process_job,
    )

    await worker.connect()
    try:
        sub = await worker.get_subscriptions()
        msgs = await worker.fetch_messages(sub[0])

        assert len(msgs) == 1
        msg = msgs[0]

        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 0

        await worker._process_task(msg)
        await asyncio.sleep(15)
        msgs = await worker.fetch_messages(sub[0])
        assert len(msgs) == 1
        msg = msgs[0]
        job_data = json.loads(msg.data.decode())
        assert job_data["name"] == "task_1"
        assert job_data["meta"]["retry_count"] == 0
        assert msg.metadata.num_delivered == 2

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


async def process_job(job_data):
    await asyncio.sleep(1)


async def process_job_with_timeout(job_data):
    await asyncio.sleep(3)


async def process_job_with_error(job_data):
    raise Exception("Test Error")
