from kafka import KafkaProducer, KafkaConsumer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Config
msk_endpoint_tls='b-2.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094,b-1.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094,b-3.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094'
msk_endpoint_plaintext='b-2.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092,b-1.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092,b-3.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092'

msk_endpoint=msk_endpoint_plaintext

TOPIC_NAME = 'ac_topic'
producer = KafkaProducer(bootstrap_servers=msk_endpoint)
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=msk_endpoint)
admin = KafkaAdminClient(bootstrap_servers=msk_endpoint)

# Functions
def send_data():
  for _ in range(100):
    producer.send(TOPIC_NAME, value=b'some_message_bytes')

def consume_data():
  for _ in range(100):
    messages = consumer.poll(max_records=1)
    return messages

def create_topic(topic_name):
  topic_list = []
  topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
  admin.create_topics(topic_list, None, False)

def print_positions():
  for topicPartition in consumer.assignment():
    topic = topicPartition.topic
    partition = topicPartition.partition
    print('Topic {} Partition {}: position {}'.format(topic, partition, consumer.position(topicPartition)))

# MAIN
create_topic(TOPIC_NAME)
#send_data()
messages = consume_data()
print(messages)

