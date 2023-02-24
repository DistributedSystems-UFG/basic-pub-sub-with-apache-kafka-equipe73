from kafka import KafkaProducer
from const import *
import sys
import random

# try:
#     topic = sys.argv[1]
# except:
#     print ('Usage: python3 producer <topic_name>')
#     exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    x = random.randint(1,2) 
    msg = 'My ' + str(i) + 'st message for topic ' + str(x)
    producer.send(str(x), value=msg.encode())
    print ('Sending message: ' + msg)

producer.flush()
