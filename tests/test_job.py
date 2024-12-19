import pytest
from nats_queue.nats_job import Job


def test_job_initialization():
    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"})

    assert job.queue_name == "my_queue"
    assert job.name == "task_1"
    assert job.data == {"key": "value"}
    assert job.meta["retry_count"] == 0
    assert job.meta["timeout"] is None


def test_job_initialization_with_delay():
    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"}, delay=100)

    assert job.delay == 100
    assert job.meta["start_time"] is not None
    assert job.meta["timeout"] is None


def test_job_initialization_without_queue_name_or_name():
    with pytest.raises(ValueError, match="Parameter 'queue_name' cannot be empty"):
        Job(queue_name="", name="test", data={"key": "value"})
    with pytest.raises(ValueError, match="Parameter 'name' cannot be empty"):
        Job(queue_name="test", name="", data={"key": "value"})


def test_job_subject():
    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"})

    assert job.subject == "my_queue.task_1"


def test_job_to_dict():
    job = Job(queue_name="my_queue", name="task_1", data={"key": "value"})
    job_dict = job.to_dict()

    assert job_dict["id"] == job.id
    assert job_dict["queue_name"] == "my_queue"
    assert job_dict["name"] == "task_1"
    assert job_dict["data"] == {"key": "value"}
    assert job_dict["meta"]["retry_count"] == 0
