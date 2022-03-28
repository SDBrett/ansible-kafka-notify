from kafka import KafkaConsumer


def main():
	consumer = KafkaConsumer(
		"mytopic",
		bootstrap_servers='localhost:9092',
		group_id='test',
	)

	print("running")

	for msg in consumer:
		print(msg)


if __name__ == "__main__":
	main()
