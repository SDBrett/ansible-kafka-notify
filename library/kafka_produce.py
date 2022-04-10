#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2021, Brett Johnson <brett@sdbrett.com>
# MIT License

from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

DOCUMENTATION = r'''
---
module: kafka_produce
short_description: Send Messages to Kafka
version_added: "1.0.0"
description: Produces new messages to Kafka cluster
author: 
  - "Brett Johnson (@sdbrett)"
options:
  topic:
    type: str
    description:
    - The topic name to publish the message to
  producer_config:
    type: dict
    elements: str
    description:
    - producer configuration parameters
    - The full list of configuration parameters are available at https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html/
    required: true
  ssl_password:
    type: str
    description:
    - Password for certificate chain file
    - Has priority over producer_config['ssl_password'] 
  sasl_plain_password:
    type: str
    description:
    - Password for sasl PLAIN and SCRAM authentication
    - Has priority over producer_config['sasl_plain_password']
  value_serializer:
    type: str
    description:
    - Serializer to use for message value
    - Accepted values: 'avro'
  value_schema:
    type: str
    description:
    - Schema for message value serialization
    - Required if value_serializer is configured
  key_serializer:
    type: str
    description:
    - Serializer to use for message key
    - Accepted values: 'avro'
  key_schema:
    type: str
    description:
    - Schema for message key serialization
    - Required if key_serializer is configured
  msg_value:
    type: str
    description:
    - Message to send
  msg_key:
    type: str
    description:
    - Value for the message key
'''

EXAMPLES = r'''
# Produce simple message
- name: Produce message
  kafka_produce:
    producer_config:
      bootstrap_servers: 'localhost:9092'
    msg_value: "my test message"
    topic: testTopic
    
# Use as a handler to send task results
- hosts: localhost
  vars:
    topic: testTopic
    producer_config:
      bootstrap_servers: 'localhost:9092'

  tasks:
    - name: create file
      file:
        path: ~/git-repos/ansible-kafka-notify/testFile
        state: touch
      register: msg
      notify: test

  handlers:
    - name: test
      kafka_produce:
        producer_config: "{{ producer_config }}"
        msg_value: "{{ msg }}"
        topic: "{{ topic }}"
'''

from ansible.module_utils.basic import AnsibleModule
import io
from kafka import KafkaProducer
import json
import avro.schema
from avro.io import DatumWriter
from jsonschema import validate

argument_spec = dict(
    topic=dict(type='str', required=True),
    producer_config=dict(type='dict', required=True, no_log=False),
    msg_value=dict(type='str', required=True),
    msg_key=dict(type='str', required=False),
    ssl_password=dict(type='str', no_log=True),
    sasl_plain_password=dict(type=str, no_log=True),
    value_serializer=dict(type=str, choices=['avro']),
    value_schema=dict(type=str),
    key_serializer=dict(type=str, choices=['avro']),
    key_schema=dict(type=str),
    registry_url=(dict(type=str)),
)

required_by = {
    'value_serializer': 'value_schema',
    'key_serializer': 'key_schema',
}


def setup_module_object():
    module = AnsibleModule(
        argument_spec=argument_spec,
        required_by=required_by,
        supports_check_mode=True,
    )
    return module


def set_sasl_password(params):
    password = params['sasl_plain_password'] if 'sasl_plain_password' in params \
        else params['producer_config']['sasl_plain_password'] if 'sasl_plain_password' in params['producer_config'] \
        else None

    return password


def set_ssl_password(params):
    password = params['ssl_password'] if 'ssl_password' in params \
        else params['producer_config']['ssl_password'] if 'ssl_password' in params['producer_config'] \
        else None

    return password


def serialize(serializer, schema, message):
    if message is None:
        return None

    if serializer == 'avro':
        schema_obj = avro.schema.parse(json.dumps(schema))
        return encode_avro(schema_obj, message)
    elif serializer == 'json':
        return encode_json(schema, message)
    else:
        return message.encode()


def encode_json(schema, message):
    message_obj = json.loads(message)
    if schema is not None:
        validate(message_obj, schema)
    return message.encode()


def encode_avro(schema, message):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(message, encoder)

    return bytes_writer.getvalue()


def produce_message(producer_config, topic, key, value):
    producer = KafkaProducer(**producer_config)
    producer.send(topic, key=key, value=value)
    producer.flush()
    producer.close()


def main():
    module = setup_module_object()
    topic = module.params['topic']
    producer_config = module.params['producer_config']
    msg_value = module.params['msg_value']
    msg_key = module.params['msg_key']
    value_serializer = module.params['value_serializer']
    key_serializer = module.params['key_serializer']
    value_schema = module.params['value_schema']
    key_schema = module.params['key_schema']

    # Set SASL auth password
    if 'sasl_mechanism' in producer_config:
        if producer_config['sasl_mechanism'] in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
            producer_config['sasl_plain_password'] = set_sasl_password(module.params)
            assert producer_config['sasl_plain_password'] is not None, \
                "sasl_plain_password is required with sasl_mechanism mechanism of {producer_config['sasl_mechanism']}"

    # Set SSL certificate password
    ssl_password = None
    if 'security_protocol' in producer_config:
        if producer_config['security_protocol'] in ('SSL', 'SASL_SSL'):
            ssl_password = set_ssl_password(module.params)

    if ssl_password is not None:
        producer_config['ssl_password'] = ssl_password

    value = serialize(value_serializer, value_schema, msg_value)

    if msg_key is not None:
        key = serialize(key_serializer, key_schema, msg_key)
    else:
        key = None

    produce_message(producer_config, key, value, topic)

    module.exit_json(msg="message successfully published")


if __name__ == "__main__":
    main()
