import json
import time
import urllib.request
from urllib.request import Request, urlopen

from kafka import KafkaProducer
from kafka.errors import KafkaError
from key import URL, AIR_KEY


class Producer():
    def __init__(self, bootstrap_servers):
        print("Kafka Producer Object Create")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers = bootstrap_servers,
                max_block_ms = 10000, 
                retries = 0, # default value
                acks = 1 # default value
                # value_serializer = lambda v: json.dumps(v).encode("utf-8")
            )
        except KafkaError as e:
            print("kafka producer - Exception during connecting to broker - {}".format(e))
            return False
        
    def stop(self):
        self.producer.close()

    def send_data(self, topic, data):        
        # Asynchronous by default
        future = self.producer.send(topic, data).add_callback(self.on_send_success).add_errback(self.on_send_error)
        
        # block until all async messages are sent
        self.producer.flush()
    
    def on_send_success(self, record_metadata):
        print("**********Send Success***********")
        print("record_metadata.topic: ", record_metadata.topic)
        print("record_metadata.partition: ", record_metadata.partition)
        print("record_metadata.offset: ", record_metadata.offset)
        pass

    def on_send_error(self, excp):
        print("**********Send Error Occur**********")
        log.error("I am an errback", exc_info=excp)


def main():
  producer = Producer(bootstrap_servers="localhost:9092")
  # porducer = KafkaProducer("localost:9092")
  while True:

    response = urllib.request.Request(URL,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
    flights = json.loads(urlopen(response).read().decode())

    for flight in flights["response"]:

      print(flight)

      producer.send_data("flight-realtime", json.dumps(flight).encode())
      # Producer.send
    print("{} Produced {} station records".format(time.time(), len(flights)))

    time.sleep(1)

if __name__ == "__main__":
  main()