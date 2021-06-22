from confluent_kafka import Consumer
import json
from datetime import datetime
import redis, json, ast

class Main:
    def __init__(self):
        conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "group1",
        'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(conf)
        self.min_commit_count = 1
        self.running = True
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.max_message = 10

    def consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            msg_count = 0
            
            ## extractor - consumer
            while self.running:
                msg, text = '', ''
                keyword_list = []

                for i in range(max_message):
                    msg = self.consumer.poll(timeout=1.0)
                    if msg is None: 
                        continue

                    if msg.error():
                        # End of partition event
                        print('error handling')
                        break
                        #raise KafkaException(msg.error())
			## transformer - change data
                        msg_json = json.loads('{}'.format(msg.value().decode('utf-8')))
                        if msg_json.get('extended_tweet'):
                            text = msg_json['extended_tweet']['full_text']
                        else:
                            text = msg_json['text']
                        text = text.encode('utf-8')
                        word_list = str(text).split(' ')

                        keys = [k.decode('utf-8') for k in self.redis.keys('*')]
                        for k in word_list:
                            k = k.lower()
                            if not k in keys:
                                continue
                            ## loader - save to redis
                            count = self.redis.get(k)
                            if k is not None:
                                count = int(count)
                                count += 1
                                print('update redis : ',k,count)
                                self.redis.set(k, count)
                if self.running:
                    break
        except:
            raise Exception('consumer loop fail')
        finally:
            ## Close down consumer to commit final offsets.
            self.consumer.commit(async=False) 
            self.consumer.close()
            self.shutdown()
    def shutdown(self):
        self.running = False

    def get_top_count(self):
        keys = [k for k in redis.keys('*')]

        count_list = {}
        for k in keys:
            count_list[k] = ast.literal_eval(redis.get(k).decode('utf-8'))['count']

        top_count = list(sorted(count_list.items, key = lambda item: item[1]))[:10]

        return top_count

if __name__ == "__main__":
    main = Main()
    topics=["bitcoin"]
    main.consume_loop(topics)
