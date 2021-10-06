from kafka import KafkaConsumer


def main():
	consumer = KafkaConsumer(
		"testTopic",
		bootstrap_servers='localhost:9092',
	)

	print("running")

	for msg in consumer:
		print(msg)


if __name__ == "__main__":
	main()
