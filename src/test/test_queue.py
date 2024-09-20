import pytest
from unittest.mock import AsyncMock, patch
from my_nats.main import Queue, Job

def test_queue_creation():
    queue = Queue(topic_name="test_topic", priorities=3)
    
    assert queue.topic_name == "test_topic"
    assert queue.priorities == 3
