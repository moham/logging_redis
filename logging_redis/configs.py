from typing import NamedTuple, Union, List


class MongoOptions(NamedTuple):
    """
    Mongodb Worker Options

    Properties:
        hosts (typing.List[str]):
            list of the mongodb nodes address.
        database (str):
            name of the mongodb database.
        collection (str):
            name of the mongodb collection.
        username (str):
            mongodb username.
        password (str):
            mongodb password.
        write_concern (int, Optional):
            mongodb write concern.
            Defaults to 0.
        connect_timeout (int, Optional):
            mongodb connection timeout in milliseconds.
            Defaults to 500.
        server_selection_timeout (int, Optional):
            mongodb server selection timeout in milliseconds.
            Defaults to 500.
        application_name (str, Optional):
            application name, uses in mongodb instance for logging.
            Defaults to 'python_logging_redis'.
        max_count (int, Optional):
            it is the maximum number of messages could be in batch insert
            request to mongodb.
            Defaults to 1000.
        time_interval (int, Optional):
            it is the maximum time in second for collect log messages before
            sending them to mongodb.
            Defaults to 5.
        retry_delay_short (float, Optional):
            short time delay in second between every reconnection,
            uses in connection error.
            Defaults to 0.1.
        retry_delay_long (float, Optional):
            long time delay in second between every reconnection,
            uses in authentication error.
            Defaults to 1.
        insert_retry_count (int, Optional):
            how many attempts to send log messages after error occurred.
            Defaults to 1.
    """
    hosts: List[str]
    database: str
    collection: str
    username: str
    password: str
    write_concern: int = 0
    connect_timeout: int = 500
    server_selection_timeout: int = 500
    application_name: str = 'python_logging_redis'
    max_count: int = 1000
    time_interval: int = 5
    retry_delay_short: int = 0.1
    retry_delay_long: int = 1
    insert_retry_count: int = 1


class ElasticOptions(NamedTuple):
    """
    Elasticsearch Worker Options

    Properties:
        hosts (typing.List[str]):
            list of the elasticsearch nodes address.
        index (str):
            elasticsearch index name.
        username (str, Optional):
            elasticsearch username.
            Defaults to None.
        password (str, Optional):
            elasticsearch password.
            Defaults to None.
        max_count (int, Optional):
            it is the maximum number of messages could be in batch insert
            request to elasticsearch.
            Defaults to 1000.
        time_interval (int, Optional):
            it is the maximum time in second for collect log messages before
            sending them to elasticsearch.
            Defaults to 5.
        retry_delay_short (float, Optional):
            short time delay in second between every reconnection,
            uses in connection error.
            Defaults to 0.1.
        retry_delay_long (float, Optional):
            long time delay in second between every reconnection,
            uses in authentication error.
            Defaults to 1.
        insert_retry_count (int, Optional):
            how many attempts to send log messages after error occurred.
            Defaults to 1.
    """
    hosts: List[str]
    index: str
    username: str = None
    password: str = None
    max_count: int = 1000
    time_interval: int = 5
    retry_delay_short: float = 0.1
    retry_delay_long: float = 1
    insert_retry_count: int = 1


class WorkerConfig(NamedTuple):
    """
    Worker Config

    Properties:
        name (str):
            name of the worker.
        type (str):
            type of the worker, could be either 'mongo' or 'elastic'.
        worker_options (Union[MongoOptions, ElasticOptions]):
            list of the worker options objects.
        queue_size (int, Optional):
            ipc queue size.
            Defaults to 1000.
    """
    name: str
    type: str
    worker_options: Union[MongoOptions, ElasticOptions]
    queue_size: int = 1000


class SubscriberConfig(NamedTuple):
    """
    Redis Subscriber Config

    Properties:
        name (str):
            name of the subscriber.
        host (str):
            redis server host, could be either hostname or ip.
        port (int):
            redis server port.
        db (int):
            redis server database.
        channel (str):
            redis pubsub channel pattern.
        workers (typing.List[WorkerConfig]):
            list of the worker config objects.
        retry_delay (int, Optional):
            delay in second between every reconnection.
            Defaults to 0.01.
    """
    name: str
    host: str
    port: int
    db: int
    channel: str
    workers: List[WorkerConfig]
    retry_delay: float = 0.01
