from confluent_kafka import Consumer, OFFSET_BEGINNING
import json


class ConsumerServer:

    def __init__(self, topic, consume_timeout, broker_properties):
        self.consumer = Consumer(broker_properties)
        self.topic = topic
        self.consumer.subscribe([topic], on_assign=self.on_assign)
        while True:
            message = self.consumer.poll(consume_timeout)
            if message is None:
                pass
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(json.loads(message.value()))

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING
        print(f"partitions assigned for {self.topic}")
        consumer.assign(partitions)


ConsumerServer(
    topic="sf-data",
    consume_timeout=0.1,
    broker_properties={
        "bootstrap.servers": "PLAINTEXT://localhost:9092",
        "group.id": "0"
    }
)
