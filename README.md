# ansible-kafka-notify

This Ansible module produces notifications to a Kafka platform.

# Use Cases

This project was started to address the following use cases
- Reduce the number of notifications sent my Ansible, single message sent, multiple consumers
- Trigger processes on external automation / orchestration platforms
- Reduce notification complexity within Ansible by decoupling the consumers

# Requirements

This module requires the following Python modules
- kafka-python

# TODO

- [ ] Support for different encodings
  - [ ] Avro
  - [ ] JSON
  - [ ] Protobuff
- [ ] Support Schema validation
- [ ] Registry Support
  - [ ] Confluent Registry
  - [ ] Apicurio
- [ ] Create tests
