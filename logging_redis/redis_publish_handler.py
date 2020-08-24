from redis import Redis
from redis.exceptions import ConnectionError
from logging import Handler, LogRecord
from time import sleep
import typing
import pickle
import json


class RedisPublishHandler(Handler):
    """
    Logging Redis Publisher Handler
    This class is a handler for logging module that sends log records
      to specific redis pubsub channel.
    """
    def __init__(
            self,
            redis_channel: str,
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            retry_count: int = 0,
            retry_delay: float = 0.001,
            serializer: str = 'pickle'
    ) -> None:
        """
        Class Constructor

        Args:
            redis_channel (str):
                redis pubsub channel name.
            redis_host (str, Optional):
                redis server host, could be either hostname or ip.
                Defaults to 'localhost'.
            redis_port (int, Optional):
                redis server port number.
                Defaults to 6379.
            redis_db (int, Optional):
                redis server database.
                Defaults to 0.
            retry_count (int, Optional):
                how many attempts to send log message after error occurred.
                Defaults to 0.
            retry_delay (float, Optional):
                delay in second between every attempt to send message.
                Defaults to 0.001.
            serializer (str, Optional):
                which serializer will be used for log records.
                could be either 'pickle' or 'json'.
                Defaults to 'pickle'.
        """
        super(RedisPublishHandler, self).__init__()
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db
        self._redis_channel = redis_channel
        self._retry_count = retry_count + 1
        self._retry_delay = retry_delay
        self._serializer_method = None
        self._redis_client = None
        self.set_serializer(serializer)

        self._redis_client = Redis(
            host=self._redis_host,
            port=self._redis_port,
            db=self._redis_db
        )

    def set_serializer(self, type_: str) -> None:
        """
        It is used to specify serializer method.
        """
        if type_ == 'pickle':
            self._serializer_method = pickle.dumps
        elif type_ == 'json':
            self._serializer_method = json.dumps
        else:
            raise TypeError(f"{type_} is not valid serializer")

    def serialize(self, record: LogRecord) -> typing.Union[bytes, str]:
        """
        It is used to serialize log messages.
        """
        return self._serializer_method(record.__dict__)

    def emit(self, record: LogRecord) -> None:
        """
        Override Handler's emit method for send message to redis channel
        """
        retry_count = self._retry_count

        while retry_count:
            try:
                self._redis_client.publish(
                    self._redis_channel,
                    self.serialize(record)
                )
            except ConnectionError:
                retry_count -= 1
                if retry_count > 0:
                    sleep(self._retry_delay)
            else:
                break
