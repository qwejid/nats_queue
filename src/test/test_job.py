import pytest
from my_nats.main import Job

def test_job_creation():
    job = Job(name="test_task", data={"key": "value"}, delay=5, meta={"priority": "high"})
    
    assert job.name == "test_task"
    assert job.data == {"key": "value"}
    assert job.delay == 5
    assert job.meta == {"priority": "high"}
    assert isinstance(job.id, str)

def test_job_to_dict():
    job = Job(name="test_task", data={"key": "value"}, delay=5, meta={"priority": "high"})
    job_dict = job.to_dict()
    
    assert job_dict["id"] == job.id
    assert job_dict["name"] == job.name
    assert job_dict["data"] == job.data
    assert job_dict["delay"] == job.delay
    assert job_dict["meta"] == job.meta