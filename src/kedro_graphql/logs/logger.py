import json
import logging
import os
from inspect import currentframe, getframeinfo
from logging import LogRecord
from typing import AsyncGenerator

import redis
import redis.asyncio as redis_asyncio
from celery.result import AsyncResult
from celery.states import READY_STATES
from starlette.concurrency import run_in_threadpool

from .json_log_formatter import JSONFormatter  # VerboseJSONFormatter also available

logger = logging.getLogger("kedro-graphql")
logger.setLevel(logging.INFO)


class RedisLogStreamPublisher(object):
    def __init__(self, topic, broker_url=None):
        self.connection = redis.Redis.from_url(broker_url)
        self.topic = topic
        if not self.connection.exists(self.topic):
            self.connection.xadd(
                self.topic, json.loads(
                    JSONFormatter().format(
                        LogRecord(
                            "kedro-graphql", 20, os.path.abspath(__file__),
                            getframeinfo(currentframe()).lineno, "Starting log stream", None, None))))
            # stream will expire in 24 hours (safety mechanism in case task_postrun fails to delete)
            self.connection.expire(self.topic, 86400)

    def publish(self, data):
        data = {k: (str(v) if isinstance(v, bool) else v) for k, v in data.items()}
        self.connection.xadd(self.topic, data)


class RedisLogStreamSubscriber(object):

    @classmethod
    async def create(cls, topic, broker_url=None):
        """Factory method for async instantiation RedisLogStreamSubscriber objects.
        """
        self = RedisLogStreamSubscriber()
        self.topic = topic
        self.broker_url = broker_url
        self.connection = await redis_asyncio.from_url(broker_url)
        return self

    async def consume(self, count=1, start_id=0):
        r = await self.connection.xread(count=count, streams={self.topic: start_id})
        return r


class KedroGraphQLLogHandler(logging.StreamHandler):
    def __init__(self, topic, broker_url=None):
        logging.StreamHandler.__init__(self)
        self.broker_url = broker_url
        self.topic = topic
        self.broker = RedisLogStreamPublisher(topic, broker_url=broker_url)
        self.setFormatter(JSONFormatter())

    def emit(self, record):
        msg = self.format(record)
        self.broker.publish(json.loads(msg))


class PipelineLogStream():

    @classmethod
    async def create(cls, task_id, broker_url=None):
        """Factory method for async instantiation PipelineLogStream objects.
        """
        self = PipelineLogStream()
        self.task_id = task_id
        self.broker_url = broker_url
        self.broker = await RedisLogStreamSubscriber().create(task_id, broker_url)
        return self

    async def consume(self) -> AsyncGenerator[dict, None]:
        start_id = 0
        while True:
            stream_data = await self.broker.consume(count=1, start_id=start_id)
            if len(stream_data) > 0:
                for id, value in stream_data[0][1]:
                    yield {"task_id": self.task_id, "message_id": id.decode(), "message": value[b"message"].decode(), "time": value[b"time"].decode()}
                # https://redis-py.readthedocs.io/en/stable/examples/redis-stream-example.html#read-more-data-from-the-stream
                start_id = stream_data[0][1][-1][0]
            else:
                # check task status - wrap blocking Celery operation in thread pool
                def check_task_status(task_id):
                    try:
                        r = AsyncResult(task_id)
                        # Check if backend is disabled/not available
                        if not hasattr(r.backend, '_get_task_meta_for'):
                            # No result backend configured, can't check status
                            # Return None to indicate we should keep streaming
                            return None
                        return r.status
                    except (AttributeError, NotImplementedError):
                        # Backend doesn't support status checking
                        return None
                
                status = await run_in_threadpool(check_task_status, self.task_id)
                # Only break if we got a valid status and it's in READY_STATES
                # If status is None (no backend), continue streaming
                if status is not None and status in READY_STATES:
                    await self.broker.connection.aclose()
                    break
