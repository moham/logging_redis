import logging
import pickle
import typing
import time
import asyncio
import motor.motor_asyncio
import aioredis
import elasticsearch
from pymongo.errors import ConnectionFailure, OperationFailure
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from logging_redis.configs import WorkerConfig, SubscriberConfig

_logger = logging.getLogger('logging_redis.async')
_logger.addHandler(logging.NullHandler())


class _AsyncMongoWorker:
    """
    Async Mongodb Worker Class
    In short, it gets messages from subscriber and then puts them in mongodb.
    """
    def __init__(
            self,
            name: str,
            loop: asyncio.AbstractEventLoop,
            queue: asyncio.Queue,
            hosts: typing.List[str],
            database: str,
            collection: str,
            username: str,
            password: str,
            write_concern: int,
            connect_timeout: int,
            server_selection_timeout: int,
            application_name: str,
            max_count: int,
            time_interval: int,
            retry_delay_short: int,
            retry_delay_long: int,
            insert_retry_count: int
    ):
        """
        Class Constructor

        Args:
            name (str):
                name of the worker.
            loop (asyncio.AbstractEventLoop):
                event loop instance.
            queue (asyncio.Queue):
                async ipc queue instance.
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
            write_concern (int):
                mongodb write concern.
            connect_timeout (int):
                mongodb connection timeout in milliseconds.
            server_selection_timeout (int):
                mongodb server selection timeout in milliseconds.
            application_name (str):
                application name, uses in mongodb instance for logging.
            max_count (int):
                it is the maximum number of messages could be in batch insert
                request to mongodb.
            time_interval (int):
                it is the maximum time in second for collect log messages before
                sending them to mongodb.
            retry_delay_short (float):
                short time delay in second between every reconnection,
                uses in connection error.
            retry_delay_long (float):
                long time delay in second between every reconnection,
                uses in authentication error.
            insert_retry_count (int):
                how many attempts to send log messages after error occurred.
        """
        self.name = name
        self.loop = loop
        self.queue = queue
        self.hosts = hosts
        self.db_name = database
        self.col_name = collection
        self.username = username
        self.password = password
        self.max_count = max_count
        self.time_interval = time_interval
        self.retry_delay_short = retry_delay_short
        self.retry_delay_long = retry_delay_long
        self.insert_retry_count = insert_retry_count + 1
        
        self.client = None
        self.database = None
        self.collection = None
        
        self.client_options = {
            'host': self.hosts,
            'username': self.username,
            'password': self.password,
            'authSource': self.db_name,
            'w': write_concern,
            'connectTimeoutMS': connect_timeout,
            'serverSelectionTimeoutMS': server_selection_timeout,
            'appname': application_name
        }

    async def connect(self) -> None:
        """
        It is used for initialize connections to mongodb instance(s)
        """
        while True:
            try:
                self.client = motor.motor_asyncio.AsyncIOMotorClient(
                    io_loop=self.loop,
                    **self.client_options
                )
                await self.client.admin.command('ismaster')
                self.database = self.client.get_database(self.db_name)
                self.collection = self.database.get_collection(self.col_name)
            except ConnectionFailure as e:
                _logger.error(f"[{self.name}] Connection failure: {e}")
                await asyncio.sleep(self.retry_delay_short)
            except OperationFailure as e:
                _logger.error(f"[{self.name}] Connection failure: {e}")
                await asyncio.sleep(self.retry_delay_long)
            else:
                _logger.info(
                    f"[{self.name}] Connected to "
                    f"{self.hosts}/{self.db_name}/{self.col_name}"
                )
                break
    
    async def insert(self, messages: typing.List[dict]) -> None:
        """
        It is used for put messages in mongodb.

        Args:
            messages (typing.List[dict]): list of log messages
        """
        retry_count = self.insert_retry_count
        while retry_count:
            try:
                await self.collection.insert_many(messages)
            except (ConnectionFailure, OperationFailure) as e:
                _logger.error(f"[{self.name}] Insert failure: {e}")
                retry_count -= 1
                if retry_count > 0:
                    _logger.info(
                        f"[{self.name}] Sleep {self.retry_delay_short} "
                        "seconds before retrying"
                    )
                    await asyncio.sleep(self.retry_delay_short)
            else:
                _logger.info(f"[{self.name}] Inserted: {len(messages)}")
                break

    async def run(self) -> None:
        """
        It is main method for this class.
        It gets messages from ipc queue and then calls insert method in
          appropriate time.
        """
        _logger.info(f"[{self.name}] Running ...")
        
        start_time = None
        log_messages = []
        
        while True:
            if not self.client:
                await self.connect()
            
            if not start_time:
                start_time = time.time()

            try:
                log_message = await asyncio.wait_for(
                    fut=self.queue.get(),
                    timeout=0.01,
                    loop=self.loop
                )
            except asyncio.TimeoutError:
                pass
            else:
                log_messages.append(pickle.loads(log_message))
                _logger.debug(f"[{self.name}] Added to list: {log_message}")

            if not log_messages:
                start_time = None
                continue

            is_max_count = len(log_messages) == self.max_count
            is_time_to_send = time.time() - start_time >= self.time_interval
            if is_max_count or is_time_to_send:
                await self.insert(log_messages)
                log_messages = []
                start_time = None


class _AsyncElasticWorker:
    """
    Async Elasticsearch Worker Class
    In short, it gets messages from subscriber and then puts them in
      elasticsearch.
    """
    def __init__(
            self,
            name: str,
            loop: asyncio.AbstractEventLoop,
            queue: asyncio.Queue,
            hosts: typing.List[str],
            index: str,
            username: str,
            password: str,
            max_count: int,
            time_interval: int,
            retry_delay_short: int,
            retry_delay_long: int,
            insert_retry_count: int
    ):
        """
        Class Constructor

        Args:
            name (str):
                name of the worker.
            queue (asyncio.Queue):
                async ipc queue instance.
            hosts (typing.List[str]):
                list of the elasticsearch nodes address.
            index (str):
                elasticsearch index name.
            username (str):
                elasticsearch username.
            password (str):
                elasticsearch password.
            max_count (int):
                it is the maximum number of messages could be in batch insert
                request to elasticsearch.
            time_interval (int):
                it is the maximum time in second for collect log messages before
                sending them to elasticsearch.
            retry_delay_short (float):
                short time delay in second between every reconnection,
                uses in connection error.
            retry_delay_long (float):
                long time delay in second between every reconnection,
                uses in authentication error.
            insert_retry_count (int):
                how many attempts to send log messages after error occurred.
        """
        self.name = name
        self.loop = loop
        self.queue = queue
        self.hosts = hosts
        self.index = index
        self.max_count = max_count
        self.time_interval = time_interval
        self.retry_delay_short = retry_delay_short
        self.retry_delay_long = retry_delay_long
        self.insert_retry_count = insert_retry_count + 1
        
        self.client = None
        
        self.client_options = {
            'hosts': self.hosts,
            'sniff_on_start': True,
            'sniff_on_connection_fail': True,
            'sniffer_timeout': 5
        }
        if username and password:
            self.client_options['username'] = username
            self.client_options['password'] = password

    async def connect(self) -> None:
        """
        It is used for initialize connections to elasticsearch instance(s)
        """
        while True:
            try:
                self.client = AsyncElasticsearch(
                    **self.client_options
                )
                await self.client.info()
            except elasticsearch.TransportError as e:
                _logger.error(f"[{self.name}] Connection Error: {e}")
                await asyncio.sleep(self.retry_delay_short)
            else:
                _logger.info(
                    f"[{self.name}] Connected to "
                    f"{self.hosts}/{self.index}"
                )
                break

    async def insert(self, messages: typing.List[dict]) -> None:
        """
        It is used for put messages in elasticsearch.

        Args:
            messages (typing.List[dict]): list of log messages
        """
        retry_count = self.insert_retry_count
        while retry_count:
            try:
                await async_bulk(self.client, messages)
            except elasticsearch.TransportError as e:
                _logger.error(f"[{self.name}] Insert Error: {e}")
                retry_count -= 1
                if retry_count > 0:
                    _logger.info(
                        f"[{self.name}] Sleep {self.retry_delay_short} "
                        "seconds before retrying"
                    )
                    await asyncio.sleep(self.retry_delay_short)
            else:
                _logger.info(f"[{self.name}] Inserted: {len(messages)}")
                break

    async def run(self) -> None:
        """
        It is main method for this class.
        It Gets messages from ipc queue and then calls insert method in
          appropriate time.
        """
        _logger.info(f"[{self.name}] Running ...")
        
        start_time = None
        log_messages = []
        
        while True:
            if not self.client:
                await self.connect()
            
            if not start_time:
                start_time = time.time()
            
            try:
                log_message = await asyncio.wait_for(
                    fut=self.queue.get(),
                    timeout=0.01,
                    loop=self.loop
                )
            except asyncio.TimeoutError:
                pass
            else:
                log_message = pickle.loads(log_message)
                log_message['_index'] = self.index
                log_messages.append(log_message)
                _logger.debug(f"[{self.name}] Added to list: {log_message}")

            if not log_messages:
                start_time = None
                continue

            is_max_count = len(log_messages) == self.max_count
            is_time_to_send = time.time() - start_time >= self.time_interval
            if is_max_count or is_time_to_send:
                await self.insert(log_messages)
                log_messages = []
                start_time = None


class _AsyncRedisSubscriber:
    """
    Async Redis Subscriber Class
    It starts workers and initializes connections to redis.
    Then gets messages from redis channels and then passes them to workers.
    """
    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            name: str,
            host: str,
            port: int,
            db: int,
            channel: str,
            retry_delay: int,
            workers: typing.List[WorkerConfig]
    ) -> None:
        """
        Class Constructor

        Args:
            loop (asyncio.AbstractEventLoop):
                event loop instance.
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
            retry_delay (int):
                delay in second between every reconnection.
        """
        self.loop = loop
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.channel = channel
        self.retry_delay = retry_delay
        self.workers = workers

        self.redis_client = None
        self.redis_channel = None
        self.queues = {
            worker.name: asyncio.Queue(worker.queue_size)
            for worker in self.workers
        }

    async def connect(self) -> None:
        """
        It is used for initialize connections to redis instance.
        """
        while True:
            try:
                self.redis_client = await aioredis.create_redis(
                    address=(self.host, self.port),
                    db=self.db,
                    loop=self.loop
                )
                await self.redis_client.ping()
                self.redis_channel, = await self.redis_client.psubscribe(
                    self.channel
                )
            except (
                ConnectionRefusedError,
                aioredis.ConnectionClosedError,
                aioredis.ConnectionForcedCloseError
            ) as e:
                _logger.error(f"[{self.name}] Connection error: {e}")
                await asyncio.sleep(self.retry_delay)
            else:
                _logger.info(
                    f"[{self.name}] Connected to "
                    f"{self.host}:{self.port}/{self.db}/{self.channel}"
                )
                break

    async def subscribe(self) -> None:
        """
        It gets messages from redis and then puts them to ipc queue.
        """
        _logger.debug(f"[{self.name}] Waiting for log messages ...")

        while True:
            if not self.redis_client:
                await self.connect()
            
            try:
                async for _, log_msg in self.redis_channel.iter():
                    put_tasks = []
                    for _, queue in self.queues.items():
                        put_tasks.append(queue.put(log_msg))
                    await asyncio.gather(*put_tasks)
                    _logger.debug(
                        f"[{self.name}] Log added to queue(s): {log_msg}"
                    )
            except asyncio.QueueFull:
                _logger.debug(f"[{self.name}] Queue is full")
            except (
                aioredis.ConnectionClosedError,
                aioredis.ConnectionForcedCloseError
            ) as e:
                _logger.error(f"[{self.name}] Connection error: {e}")
                self.redis_client = None

    async def run(self) -> None:
        """
        It is main method for this class.
        It starts workers and then calls subscriber method.
        """
        _logger.info(f"[{self.name}] Running ...")

        running_workers = []
        for worker in self.workers:
            if worker.type == 'mongo':
                worker_class = _AsyncMongoWorker
            elif worker.type == 'elastic':
                worker_class = _AsyncElasticWorker
            else:
                worker_class = None

            if worker_class:
                running_workers.append(
                    worker_class(
                        name=worker.name,
                        loop=self.loop,
                        queue=self.queues[worker.name],
                        **worker.worker_options._asdict()
                    ).run()
                )

        await asyncio.gather(self.subscribe(), *running_workers)


class AsyncRedisSubscribers:
    """
    Async Redis Subscribers Class
    The only class available to end user for config and start server.
    It gets configurations from user and then starts subscribers.

    Example:
        AsyncRedisSubscribers(
            configs=[
                SubscriberConfig(
                    name='logger_redis_test_1',
                    host='localhost',
                    port=6379,
                    db=0,
                    channel='loggerRedis.test1',
                    workers=[
                        WorkerConfig(
                            name='mongo_worker',
                            type='mongo',
                            queue_size=10000,
                            worker_options=MongoOptions(
                                hosts=['localhost:27017'],
                                database='python_logging_redis',
                                collection='loggerRedisTest1',
                                username='user',
                                password='secret'
                            )
                        )
                    ]
                ),
                SubscriberConfig(
                    name='logger_redis_test_2',
                    host='localhost',
                    port=6379,
                    db=0,
                    channel='loggerRedis.test2',
                    workers=[
                        WorkerConfig(
                            name='elastic_worker',
                            type='elastic',
                            queue_size=10000,
                            worker_options=ElasticOptions(
                                hosts=['localhost:9200'],
                                index='python_logging_redis'
                            )
                        )
                    ]
                )
            ]
        ).run()
    """
    
    def __init__(
            self,
            configs: typing.List[SubscriberConfig]
    ) -> None:
        """
        Class Constructor

        Args:
            configs (typing.List[SubscriberConfig]):
                list of the subscriber config objects
        """
        self._configs = configs
        self._loop = None
    
    def run(self) -> None:
        """
        It is main method for this class.
        It starts subscribers and waits for them to finish.
        """
        if self._loop is not None:
            return None
        
        subscribers = []
        self._loop = asyncio.get_event_loop()

        for config in self._configs:
            subscribers.append(
                _AsyncRedisSubscriber(
                    loop=self._loop,
                    **config._asdict()
                ).run()
            )

        all_subs = asyncio.gather(*subscribers)
        self._loop.run_until_complete(all_subs)
        self._loop.close()
