#!/usr/bin/env python

import pika
import pickle
import json
from kafka import KafkaProducer

# Rabbit Consumer
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

#Kafka Producer
producer = KafkaProducer(bootstrap_servers='192.168.1.7:9092')

def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    obj = pickle.loads(body)

    # Drop trace_context before marshalling
    if isinstance(obj, dict):
        if obj.has_key('trace_context'):
            del obj['trace_context']

    data = json.dumps(obj,sort_keys=True, indent=4)
    producer.send('test', data)

channel.basic_consume(queue='test',
                      auto_ack=True,
                      on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
