from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

# Config
msk_endpoint_tls='b-2.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094,b-1.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094,b-3.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9094'
msk_endpoint_plaintext='b-2.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092,b-1.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092,b-3.alec-mykafka-poc.mktj7c.c4.kafka.ap-southeast-2.amazonaws.com:9092'

msk_endpoint=msk_endpoint_plaintext

# Functions
def send_data():
    producer = KafkaProducer(bootstrap_servers=msk_endpoint)
    for _ in range(100):
        producer.send('ac_topic', b'some_message_bytes')


def create_topic(topic_name):
    admin = KafkaAdminClient(bootstrap_servers=msk_endpoint)
    topic_list = []
    topic_list.append(NewTopic(name="ac_topic", num_partitions=1, replication_factor=1))
    admin.create_topics(topic_list, None, False)

# MAIN
send_data()