import pytest
from nats_queue.main import Job

@pytest.mark.asyncio
async def test_job_creation():
    job = Job(queue_name="my_queue", name="test_task", data={"key": "value"}, delay=5, meta={"priority": "high"})
    
    assert job.name == "test_task"
    assert job.data == {"key": "value"}
    assert job.delay == 5
    assert job.meta == {"priority": "high"}
    assert isinstance(job.id, str)
    assert job.subject == "my_queue.test_task"

def test_job_to_dict():
    job = Job(queue_name="my_queue", name="test_task", data={"key": "value"}, delay=5, meta={"priority": "high"})
    job_dict = job.to_dict()
    
    assert job_dict["id"] == job.id
    assert job_dict["name"] == job.name
    assert job_dict["data"] == job.data
    assert job_dict["delay"] == job.delay
    assert job_dict["meta"] == job.meta