import unittest
import pickle
import json
from unittest.mock import Mock
from redis.exceptions import ConnectionError
from logging_redis import RedisPublishHandler
from logging import makeLogRecord


class HandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.retry_count = 3
        self.handler = RedisPublishHandler(
            redis_channel="test.logger",
            retry_count=self.retry_count
        )
        self.log_record = makeLogRecord({})
        self.log_record_dict = self.log_record.__dict__

    def connection_error_side_effect(self, *args) -> None:
        self.retry_count -= 1
        raise ConnectionError(args)

    def test_invalid_types(self) -> None:
        with self.assertRaises(TypeError):
            RedisPublishHandler(
                redis_channel='test',
                redis_host=4
            ).emit(makeLogRecord({}))

        with self.assertRaises(ValueError):
            RedisPublishHandler(
                redis_channel='test',
                redis_port='g'
            ).emit(makeLogRecord({}))

        with self.assertRaises(TypeError):
            RedisPublishHandler(
                redis_channel='test',
                retry_count='2'
            )

        with self.assertRaises(TypeError):
            RedisPublishHandler(
                redis_channel='test',
                retry_count=2,
                retry_delay='2'
            ).emit(makeLogRecord({}))

        with self.assertRaises(TypeError):
            RedisPublishHandler(
                redis_channel='test',
                serializer='test'
            )

        with self.assertRaises(TypeError):
            RedisPublishHandler(
                redis_channel='test',
                serializer=2
            )

    def test_creation_object(self) -> None:
        self.assertIsInstance(self.handler, RedisPublishHandler)

    def test_set_serializer(self) -> None:
        handler = RedisPublishHandler('test')
        handler.set_serializer('json')
        self.assertIs(handler._serializer_method, json.dumps)
        handler.set_serializer('pickle')
        self.assertIs(handler._serializer_method, pickle.dumps)

    def test_pickle_serialization(self) -> None:
        serialized = self.handler.serialize(self.log_record)
        self.assertDictEqual(
            pickle.loads(serialized),
            self.log_record_dict
        )

    def test_json_serialization(self) -> None:
        self.handler.set_serializer('json')
        serialized = self.handler.serialize(self.log_record)
        record_dict = json.loads(serialized)
        record_dict['args'] = tuple()
        self.assertDictEqual(
            record_dict,
            self.log_record_dict
        )

    def test_set_serializer_exception(self) -> None:
        with self.assertRaises(TypeError):
            self.handler.set_serializer('not_support_serializer')

    def test_retry_count(self) -> None:
        self.handler._redis_client.publish = Mock(
            side_effect=self.connection_error_side_effect
        )
        self.handler.emit(self.log_record)
        self.assertEqual(self.retry_count, -1)
