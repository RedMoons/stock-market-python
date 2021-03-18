from kafka import KafkaConsumer
import json

class Main:
    def __init__(self):
        pass

    def execute(self):
        print("start main.py")

        # extractor - consumer
        consumer = KafkaConsumer('stockmarket', bootstrap_servers='localhost:9092', max_poll_records=100)

        # while True:

        for msg in consumer:                                                                                                                                                                                    
            m = json.loads(msg[6].decode('utf-8'))                                                                                                                                                              
            print(m['text'])


        # transformer - change data

        # loader - save to couchbase - nosql

if __name__ == "__main__":
    main = Main()
    main.execute()
