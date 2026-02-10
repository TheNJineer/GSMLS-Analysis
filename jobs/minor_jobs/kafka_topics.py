import sys
import argparse
import kafka.errors
from kafka.admin import NewTopic
from kafka.admin.client import KafkaAdminClient


def parse_args():

    parser = argparse.ArgumentParser(description="Create necessary Kafka topics if they aren't available")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--kafka_conn", required=True)

    return parser.parse_args(['--topic', 'res_properties', "--kafka_con", "True"])


def create_kafka_topics(topic: str = 'res_properties', status: bool = True):

    topic_list = ["prop_images", topic]

    if status is True:

        admin_client = KafkaAdminClient(
            bootstrap_servers=["broker-1:9092", "broker-2:9092", "broker-3:9092"],
            client_id="check_topic",
        )

        available_topics = admin_client.list_topics()
        print(f' ==== CURRENT EXISTING TOPICS: {available_topics} ==== ')

        for t in topic_list:

            try:
                if t not in available_topics:
                    topic_obj = NewTopic(
                        name=t,
                        num_partitions=3,
                        replication_factor=2,
                        topic_configs={"cleanup.policy": "compact"},  # Look into what other configs I need
                    )
                    admin_client.create_topics(
                        new_topics=[topic_obj], validate_only=False, timeout_ms=1500
                    )
                    print(f'TOPIC {t} CREATED IN APACHE KAFKA TOPIC LIST ==== ')
                else:
                    print(f' ==== TOPIC {t} ALREADY EXISTS IN KAFKA ==== ')

            except kafka.errors.TopicAlreadyExistsError:
                print(f' ==== TOPIC {t} ALREADY EXISTS IN KAFKA ==== ')


if __name__ == '__main__':

    args = parse_args()
    create_kafka_topics(args.topic, bool(args.kafka_conn))
    sys.exit(0)

