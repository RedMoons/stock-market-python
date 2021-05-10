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
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
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
                # transformer - change data
                msg_json = json.loads('{}'.format(msg.value().decode('utf-8')))

                text, ts = '', ''
                if msg_json.get('extended_tweet'):
                    text = msg_json['extended_tweet']['full_text'] 
                    print(msg_json['extended_tweet']['full_text'])
                else:
                    text = msg_json['text'] 
                    print(msg_json['text'])
                    
                ts = datetime.strptime(msg_json['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime('%Y-%m-%d %H:%M:%S')
                text_list = text.split(' ')

                keys = [k for k in redis.keys('*')]

                for t in text_list:
                    key = self.redis.get(t).decode('utf-8')
                    if not key:
                        continue

                    # loader - save to redis
                    # 1. check exist in redis list
                    # 2. get target data as json
                    # 3. update target data
                    key = key.lower()
                    contents = ''
                    if not key in keys:
                        continue

                    contents = ast.literal_eval(redis.get(key).decode('utf-8'))
                    contents['count'] += 1
                    contents['record'].append(ts)
                    redis.set(key,str(contents))

                msg_count += 1
                if msg_count % self.min_commit_count == 0:
                    self.consumer.commit(async=False)
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def shutdown():
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
