from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import json

import avro
from ansible_collections.community.general.tests.unit.compat.mock import patch
import kafka_produce
from ansible_collections.community.general.tests.unit.plugins.modules.utils import AnsibleFailJson, \
    ModuleTestCase, set_module_args


class TestKafkaProduceModule(ModuleTestCase):

    def setUp(self):
        super(TestKafkaProduceModule, self).setUp()
        self.module = kafka_produce

    def tearDown(self):
        super(TestKafkaProduceModule, self).tearDown()

    def test_without_required_parameters(self):
        """Failure must occurs when all parameters are missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({})
            self.module.main()

    def test_without_producer_config_parameter(self):
        """Failure must occurs when producer_config is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'topic': 'topicName',
                'msg_value': 'myMessage'
            })
            self.module.main()

    def test_without_topic_parameter(self):
        """Failure must occurs when topic is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'msg_value': 'myMessage'
            })
            self.module.main()

    def test_without_message_parameter(self):
        """Failure must occurs when message is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'topic': 'myTopic'
            })
            self.module.main()

    def test_missing_serializer_required(self):

        """Failure must occurs when topic is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'msg_value': 'myMessage',
                'topic': 'mytopic',
                'value_serializer': 'avro'
            })
            self.module.main()

        """Failure must occurs when topic is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'msg_value': 'myMessage',
                'topic': 'mytopic',
                'key_serializer': 'avro',
            })
            self.module.main()


class TestSSLPasswordPriorities(ModuleTestCase):

    def setUp(self):
        super(TestSSLPasswordPriorities, self).setUp()
        self.module = kafka_produce

    def tearDown(self):
        super(TestSSLPasswordPriorities, self).tearDown()

    def test_sasl_password_parameter_priority(self):
        sasl_password_test_params = [
            {
                'module_params': {
                    'producer_config': {
                        'sasl_plain_password': 'password1'
                    },
                    'sasl_plain_password': 'password2'
                },
                'expect': 'password2'
            },
            {'module_params': {
                'producer_config': {
                    'sasl_plain_password': 'password1'
                }
            },
                'expect': 'password1'
            },
            {'module_params': {
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'sasl_plain_password': 'password2'
            },
                'expect': 'password2'
            },
            {'module_params': {
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                }
            },
                'expect': None
            }
        ]

        for item in sasl_password_test_params:
            password = kafka_produce.set_sasl_password(
                item['module_params'])
            self.assertEqual(password, item['expect'])

    def test_ssl_password_parameter_priority(self):
        ssl_password_test_params = [
            {
                'module_params': {
                    'producer_config': {
                        'ssl_password': 'password1'
                    },
                    'ssl_password': 'password2'
                },
                'expect': 'password2'
            },
            {'module_params': {
                'producer_config': {
                    'ssl_password': 'password1'
                }
            },
                'expect': 'password1'
            },
            {'module_params': {
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'ssl_password': 'password2'
            },
                'expect': 'password2'
            },
            {'module_params': {
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                }
            },
                'expect': None
            }
        ]

        for item in ssl_password_test_params:
            password = kafka_produce.set_ssl_password(
                item['module_params'])
            self.assertEqual(password, item['expect'])


class TestAvroEncoding(ModuleTestCase):

    def setUp(self):
        super(TestAvroEncoding, self).setUp()
        self.module = kafka_produce

    def tearDown(self):
        super(TestAvroEncoding, self).tearDown()

    def test_serialize(self):
        """
        to test:
        - Schema and string do not align
        - Schema invalid JSON format
        - No Schema
        - Schema not avro suitable
        """

        success_test_cases = [
            {
                'schema': {"fields": [{"name": "id", "type": "string"}, {"name": "payload", "type": {
                    "fields": [{"name": "temperature", "type": "float"}, {"name": "humidity", "type": "float"},
                               {"name": "pressure", "type": "float"}], "name": "payload", "type": "record"}}],
                           "name": "Metrics", "namespace": "com.sdbrett", "type": "record"},
                'string': {"id": "2134", "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            }
        ]
        error_test_cases = [
            # No Schema supplied
            {
                'schema': None,
                'string': {"id": "2134", "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            },
            # Bad Schema string
            {
                'schema': {"fields": [{"name": "id", "type": "string"}, {"name": "payload", "type": {
                    "fields": [{"name": "temperature", "type": "float"}, {"name": "humidity", "type": "float"},
                               {"name": "pressure", "type": "float"}], }}], "name": "Metrics",
                           "namespace": "com.sdbrett", "type": "record"},
                'string': {"id": "2134", "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            },
            # Schema and value mismatch
            {
                'schema': {"fields": [{"name": "id", "type": "string"}, {"name": "payload", "type": {
                    "fields": [{"name": "temperature", "type": "float"}, {"name": "humidity", "type": "float"},
                               {"name": "pressure", "type": "float"}], "name": "payload", "type": "record"}}],
                           "name": "Metrics", "namespace": "com.sdbrett", "type": "record"},
                'string': {"id": 2134, "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            },
            {
                'schema': {"fields": [{"name": "id", "type": "string"}, {"name": "payload", "type": {
                    "fields": [{"name": "temperature", "type": "float"}, {"name": "humidity", "type": "float"},
                               {"name": "pressure", "type": "float"}], "name": "payload", "type": "record"}}],
                           "name": "Metrics", "namespace": "com.sdbrett", "type": "record"},
                'string': {"id": "2134", "badfield": "test",
                           "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            }
        ]

        none_test_result = kafka_produce.serialize(serializer='avro', schema=None, message=None)
        self.assertEqual(none_test_result, None)

        for item in success_test_cases:
            kafka_produce.serialize('avro', schema=item['schema'], message=item['string'])
            kafka_produce.encode_avro(avro.schema.parse(json.dumps(item['schema'])), message=item['string'])

        with self.assertRaises(Exception):
            for item in error_test_cases:
                kafka_produce.serialize('avro', schema=item['schema'], message=item['string'])
                kafka_produce.encode_avro(schema=json.dumps(item['schema']), message=item['string'])


class TestJsonEncoding(ModuleTestCase):

    def setUp(self):
        super(TestJsonEncoding, self).setUp()
        self.module = kafka_produce

    def tearDown(self):
        super(TestJsonEncoding, self).tearDown()

    def test_serialize(self):
        """
        to test:
        - Schema and string do not align
        - Schema invalid JSON format
        - No Schema
        - Schema not json suitable
        """

        success_test_cases = [
            {
                'schema': {"type": "object", "properties": {"price": {"type": "number"}, "name": {"type": "string"},},},
                'string': '{"name": "Eggs", "price":  34.99}'
            }
        ]
        error_test_cases = [
            # No Schema supplied
            {
                'schema': None,
                'string': {"id": "2134", "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            },
            # TODO Bad Schema string
            {
                'schema': {"type": "object", "properties": {"price": {"type": "number"}, "name": {"type": "string"},},},
                'string': '{"name": "Eggs", "price":  "3a4.99"}'
            },
            {
                'schema': {"type": "object", "properties": {"price": {"type": "number"},  "name": {"type": "string"},},},
                'string': {"id": 2134, "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            },
            {
                'schema': {"additionalProperties": False, "type": "object", "properties": {"price": {"type": "number"}, "name": {"type": "string"},},},
                'string': {"id": "2134", "badfield": "test", "payload": {"temperature": 23.2, "humidity": 94.3, "pressure": 122.3}}
            }
        ]

        none_test_result = kafka_produce.serialize(serializer='json', schema=None, message=None)
        self.assertEqual(none_test_result, None)

        for item in success_test_cases:
            kafka_produce.serialize('json', schema=item['schema'], message=item['string'])
            kafka_produce.encode_json(item['schema'], message=item['string'])

        with self.assertRaises(Exception):
            for item in error_test_cases:
                kafka_produce.serialize('json', schema=item['schema'], message=item['string'])
                kafka_produce.encode_json(item['schema'], message=item['string'])


@patch("kafka_produce.KafkaProducer")
def test_produce_message(mock_producer_class):
    producer_config = {
        'bootstrap_servers': 'localhost:9092'
    }

    mock_producer = mock_producer_class()
    key = 'test'
    message = "test"

    kafka_produce.produce_message(producer_config, 'topic', key.encode(), message.encode())

    mock_producer.send.assert_called_with('topic', key=key.encode(), value=message.encode(), )
    mock_producer.flush.assert_called_once()
