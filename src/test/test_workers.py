import pytest
import asyncio
from nats_queue.main import Worker, Job, Queue


@pytest.mark.asyncio
async def test_worker_initialization():
    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 30}, processor_callback=process_job)

    assert worker.topic_name == "my_queue"
    assert worker.concurrency == 3
    assert worker.rate_limit == {"max": 5, "duration": 30}
    assert worker.max_retries == 3
    assert isinstance(worker.queue, asyncio.Queue)

@pytest.mark.asyncio
async def test_worker_connect_and_close():
    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 30}, processor_callback=process_job)
    
    await worker.connect()
    assert worker.nc is not None
    await worker.close()
    assert worker.nc.is_closed

@pytest.mark.asyncio
async def test_worker_fetch_messages():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    jobs = [Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"}) for i in range(1, 6)]
    await queue.addJobs(jobs)

    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 30}, processor_callback=process_job)
    await worker.connect()

    sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")

    try:
        
        await worker.fetch_messages(sub)

        assert not worker.queue.empty()
        assert worker.queue.qsize() == 5
        fetched_job_data, _ = await worker.queue.get()
        assert fetched_job_data['name'] == "task_1"
        assert worker.queue.qsize() == 4
        fetched_job_data, _ = await worker.queue.get()
        assert fetched_job_data['name'] == "task_2"

    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()

@pytest.mark.asyncio
async def test_worker_process_task_success():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job_data = {"name": "task_1", "data": {"key": "value"}, "retry_count": 0}
    job = Job(queue_name="my_queue", name=job_data['name'], data=job_data['data'])
    await queue.addJob(job)
    
    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 30}, processor_callback=process_job)
    await worker.connect()

    sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")
    await worker.fetch_messages(sub)
    job_data, msg = await worker.queue.get()

    await worker._process_task(job_data, msg)

    assert job_data['retry_count'] == 0

    await queue.js.delete_stream(queue.topic_name)
    await queue.close()
    await worker.close()


@pytest.mark.asyncio
async def test_worker_process_task_with_retry():

    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job_data = {"name": "task_1", "data": {"key": "value"}, "retry_count": 0}
    job = Job(queue_name="my_queue", name=job_data['name'], data=job_data['data'])
    await queue.addJob(job)

    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 10}, processor_callback=process_job_with_error)
    await worker.connect()
    try:
        sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")
        await worker.fetch_messages(sub)
        job_data, msg = await worker.queue.get()

        await worker._process_task(job_data, msg)

        assert job_data['retry_count'] == 1
        assert worker.queue.qsize() == 1
    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()

@pytest.mark.asyncio
async def test_worker_process_task_exceeds_max_retries():
    queue = Queue(topic_name="my_queue")
    await queue.connect()

    job_data = {"name": "task_1", "data": {"key": "value"}, "retry_count": 3}
    job = Job(queue_name="my_queue", name=job_data['name'], data=job_data['data'])
    job.retry_count = job_data['retry_count']
    await queue.addJob(job)

    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 10}, processor_callback=process_job)
    await worker.connect()
    
    try:
        sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")
        await worker.fetch_messages(sub)
        assert worker.queue.qsize() == 1
        job_data, msg = await worker.queue.get()

        await worker._process_task(job_data, msg)

        assert job_data['retry_count'] == 3
        await worker.fetch_messages(sub)
        assert worker.queue.empty()

        await worker.fetch_messages(sub)
        assert worker.queue.qsize() == 0
    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()

@pytest.mark.asyncio
async def test_worker_process_task_with_timeout():

    queue = Queue(topic_name="my_queue")
    await queue.connect()
    job_data = {"name": "task_1", "data": {"key": "value"}, "retry_count": 0}
    job = Job(queue_name="my_queue", name=job_data['name'], data=job_data['data'])
    await queue.addJob(job)

    worker = Worker(topic_name="my_queue", concurrency=3, rate_limit={"max": 5, "duration": 5}, processor_callback=process_job_with_timeout)
    await worker.connect()
    try:
        sub = await worker.js.pull_subscribe(f"{worker.topic_name}.*.*")
        await worker.fetch_messages(sub)
        job_data, msg = await worker.queue.get()

        await worker._process_task(job_data, msg)

        assert job_data['retry_count'] == 1
        assert worker.queue.qsize() == 1
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
        processor_callback=process_job, priorities=queue.priorities
    )
    
    await worker.connect()
    try:
        subscriptions = await worker.get_subscriptions()

        worker_name = []
        for sub in subscriptions:
            info = await sub.consumer_info()
            filter_subject = info.config.filter_subject
            worker_name.append(filter_subject)

        assert  worker_name == ['my_queue.*.1', 'my_queue.*.2', 'my_queue.*.3']
    finally:
        await queue.js.delete_stream(queue.topic_name)
        await queue.close()
        await worker.close()


async def process_job(job_data):
    await asyncio.sleep(1)

async def process_job_with_timeout(job_data):
    await asyncio.sleep(10)

async def process_job_with_error(job_data):
    raise Exception("Test Error")