wait_for_kafka() {
  RETRY_COUNT=$1
  MAX_RETRIES=$2
	until [ $RETRY_COUNT -gt $MAX_RETRIES ]
	do
	  echo "Testing if Kafka is running, attempt ${RETRY_COUNT} of ${MAX_RETRIES}"
	  if ! docker-compose --file ./hack/docker-compose.yml exec broker kafka-topics --bootstrap-server broker:9092 --describe --topic _confluent_balancer_api_state  > /dev/null; then
		((RETRY_COUNT=RETRY_COUNT+1))
	  else
		printf '%s\n' "Info: Validated that Kafka is running"
		break
	  fi
	  if [[ $RETRY_COUNT -gt $MAX_RETRIES ]]; then
		 printf '%s\n' "Error: Unable to validate Kafka is running" >&2
		 exit 1
	  else
		sleep 10
	  fi
	done
}

create_test_topic(){
  printf '%s\n' "Info: Creating test topic"
  if ! var=$(docker-compose --file ./hack/docker-compose.yml exec broker kafka-topics --bootstrap-server broker:9092 --create --topic test); then
    if [[ $var == *"Topic 'test' already exists"* ]]; then
      printf '%s\n' "Info: Topic already exists"
    else
      printf '%s\n' $var >&2
      exit 1
    fi
  else
    printf '%s\n' "Info: Created topic"
  fi
}

$*