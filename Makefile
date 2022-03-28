# Current Operator version
VERSION ?= 0.0.1
ANSIBLE_VERSION ?= 4.6.0
IMG ?= kafka-produce-ansible-${ANSIBLE_VERSION}:${VERSION}
COMPOSER ?= $(shell which docker-compose)
WORK_DIR ?= /opt/app-root/src
RETRY_COUNT=1
MAX_RETRIES=6
MODULE_ROOT_DIR = $(shell pwd)
SHELL ?= /bin/sh
#include ./hack/helper.sh

standup-kafka:
	@echo "Starting Kafka"
	$(COMPOSER) --file $(MODULE_ROOT_DIR)/hack/docker-compose.yml up -d
	$(MODULE_ROOT_DIR)/hack/helper.sh wait_for_kafka 1 6
	$(MODULE_ROOT_DIR)/hack/helper.sh create_test_topic

stop-kafka:
	$(COMPOSER) --file $(MODULE_ROOT_DIR)/hack/docker-compose.yml stop

clean-up:
	$(COMPOSER) --file $(MODULE_ROOT_DIR)/hack/docker-compose.yml down
