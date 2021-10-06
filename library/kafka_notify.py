#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2021, Brett Johnson <brett@sdbrett.com>
# MIT License

from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

DOCUMENTATION = r'''
---
module: kafka_notify
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
  msg:
    type: str
    description:
    - Message to send
'''

EXAMPLES = r'''
# Produce simple message
- name: Produce message
  kafka_notify:
    producer_config:
      bootstrap_servers: 'localhost:9092'
    msg: "my test message"
    topic: testTopic
    
# Use as a handler to send task results
- hosts: localhost
  vars:
    topic: testTopic
    client_properties:
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
      kafka_notify:
        producer_config: "{{ client_properties }}"
        msg: "{{ msg }}"
        topic: "{{ topic }}"
'''

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
import traceback

try:
    from kafka import KafkaProducer
except ImportError:
    HAS_ANOTHER_LIBRARY = False
    ANOTHER_LIBRARY_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_ANOTHER_LIBRARY = True


def main():
    module = AnsibleModule(
        argument_spec=dict(
            topic=dict(type='str'),
            producer_config=dict(type='dict', required=True, no_log=False),
            ssl_password=dict(type='str', no_log=True),
            sasl_plain_password=dict(type=str, no_log=True),
            msg=dict(type='str'),
        ),
        supports_check_mode=True,
    )

    if not HAS_ANOTHER_LIBRARY:
        module.fail_json(
            msg=missing_required_lib('kafka'),
            exception=ANOTHER_LIBRARY_IMPORT_ERROR)

    topic = module.params['topic']
    producer_config = module.params['producer_config']
    msg = module.params['msg']

    if producer_config['sasl_mechanism'] in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
        if module.params['sasl_plain_password'] is not None:
            producer_config['sasl_plain_password'] = module.params['sasl_plain_password']

    if producer_config['security_protocol'] in ('SSL', 'SASL_SSL'):
        if module.params['ssl_password'] is not None:
            producer_config['ssl_password'] = module.params['ssl_password']

    producer = KafkaProducer(**producer_config)
    producer.send(topic, msg.encode())
    producer.flush()
    producer.close()
    module.exit_json(msg="OK")


if __name__ == "__main__":
    main()
