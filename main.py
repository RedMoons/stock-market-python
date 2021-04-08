from confluent_kafka import Consumer
import json
from datetime import datetime

class Main:
    def __init__(self):
        conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "group1",
        'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(conf)
        self.min_commit_count = 10
        self.running = True
    def consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            msg_count = 0
            
            # extractor - consumer
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
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
                    msg_json = json.loads('{}'.format(msg.value().decode('utf-8')))

                    if msg_json.get('extended_tweet'):
                        print(msg_json['extended_tweet']['full_text'])
                    else:
                        print(msg_json['text'])
                    # loader - save to couchbase - nosql
                    msg_count += 1
                    if msg_count % self.min_commit_count == 0:
                        self.consumer.commit(async=False)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown():
        self.running = False

if __name__ == "__main__":
    main = Main()
    topics=["stockmarket"]
    main.consume_loop(topics)
