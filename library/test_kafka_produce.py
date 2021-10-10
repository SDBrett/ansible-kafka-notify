from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import json
import pytest
from ansible_collections.community.general.tests.unit.compat.mock import Mock, patch
import kafka_produce
from ansible_collections.community.general.tests.unit.plugins.modules.utils import AnsibleExitJson, AnsibleFailJson, \
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
                'msg': 'myMessage'
            })
            self.module.main()

    def test_without_topic_parameter(self):
        """Failure must occurs when topic is missing"""
        with self.assertRaises(AnsibleFailJson):
            set_module_args({
                'producer_config': {
                    'bootstrap_servers': 'localhost:9092'
                },
                'msg': 'myMessage'
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

    def test__sasl_password_parameter_priority(self):
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
