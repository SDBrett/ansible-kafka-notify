#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = """
module = kafka_notify
short_description: Send Messages to Kafka
description: Produces new messages to Kafka cluster
author: "Brett Johnson (@sdbrett)"
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
    required: true
  msg:
    type: str
    description:
      - Message to send
    version_added: 1.0.0
"""

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


	producer = KafkaProducer(bootstrap_servers='localhost:9092')
	producer.send(topic, msg.encode())
	producer.flush()
	producer.close()


	# topic = 'testTopic'
	# config = {
	# 	'bootstrap_servers': 'localhost:9092'
	# }
	# producer = KafkaProducer(**config)
	# msg = 'testMessage'
	#
	# producer.send(topic, b'%b' % msg.encode())
	# producer.flush()
	# producer.close()
	module.exit_json(msg="OK")


if __name__ == "__main__":
	main()
