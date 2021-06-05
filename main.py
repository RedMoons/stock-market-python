from confluent_kafka import Consumer
import json
from datetime import datetime
import redis

class Main:
    def __init__(self):
        conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "group1",
        'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(conf)
        self.min_commit_count = 1
        self.running = True
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
    def consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            msg_count = 0
            
            ## extractor - consumer
            while self.running:
                msg, text = '', ''
                keyword_list = []
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: 
                    continue

                if msg.error():
                    # End of partition event
                    print('error handling')
                    #raise KafkaException(msg.error())
                else:
                    ## transformer - change data
                    msg_json = json.loads('{}'.format(msg.value().decode('utf-8')))

                    #print('msg_json', msg_json)
                    if msg_json.get('extended_tweet'):
                        text = msg_json['extended_tweet']['full_text']
                    else:
                        text = msg_json['text']
                    text = text.encode('utf-8')
                    word_list = str(text).split(' ')

                    print('word_list', word_list)


                    keys = [k for k in self.redis.keys('*')]
                    for k in word_list:
                        k = k.lower()
                        if not k in keys:
                            continue
                        ## loader - save to redis
                        count = self.redis.get(k)
                        if k is not None and obj is not None:

                            
                            count = int(count)
                            count += 1
                            print('update redis : key=%s, count=%s',k,count)
                            self.redis.set(k, count)

                            msg_count += 1
                            if msg_count % self.min_commit_count == 0:
                                self.consumer.commit(async=False)
        finally:
            ## Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown():
        self.running = False

if __name__ == "__main__":
    main = Main()
    topics=["bitcoin"]
    main.consume_loop(topics)
