import pytest
from my_nats.main import Job

def test_job_creation():
    job = Job(name="test_task", data={"key": "value"}, delay=5, meta={"priority": "high"})
    
    assert job.name == "test_task"
    assert job.data == {"key": "value"}
    assert job.delay == 5
    assert job.meta == {"priority": "high"}
    assert isinstance(job.id, str)