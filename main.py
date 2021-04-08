from confluent_kafka import Consumer
import json
from datetime import datetime

class Main:
    def __init__(self):
        conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "group1",
        'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(conf)
        self.running = True
        self.min_commit_count = 100

    def consume_loop(consumer, topics):
        try:
            consumer.subscribe(topics)

            msg_count = 0
            # extractor - consumer
            while running:
                msg = consumer.poll(timeout=1.0)
                if msg is None: 
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # transformer - change data
                    # msg_process(msg)

                    # loader - save to couchbase - nosql
                    msg_count += 1
                    if msg_count % min_commit_count == 0:
                        consumer.commit(async=False)
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

    def shutdown():
        running = False

if __name__ == "__main__":
    main = Main()
    main.consume_loop("stockmarket")
