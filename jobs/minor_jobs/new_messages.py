import argparse
import sys
from kafka.structs import TopicPartition
from gsmls.utility_func import create_kafka_consumer, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Check for new messages in TopicPartitons')
    parser.add_argument("--topic", required=True)
    parser.add_argument("--prop_type", required=True)

    # return parser.parse_args(['--topic', 'res_properties'])
    return parser.parse_args()


def new_msgs_available(topic):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic}_msg_check", "data_consumer")

    # Check the partitions in the consumer. Returns a set of partition ids
    partitions = cons.partitions_for_topic(topic)
    print(f' ==== CURRENT PARTITIONS AVAILABLE: {partitions} ==== ')

    try:
        if not partitions:
            print(f" ==== NO PARTITIONS FOUND FOR TOPIC {topic} ==== ")

            raise AttributeError(f"No partitions created for {topic}")

        # Create list of TopicPartition objects to check end offsets
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        offset_dict.update({f"{tp}": False for tp in topic_partitions})

        # Returns a dict of partitions and their end offsets in key-value pairs
        end_offsets = cons.end_offsets(topic_partitions)

        for tp in topic_partitions:
            # tp is the topic partition object
            committed = cons.committed(tp)
            latest = end_offsets[tp]

            if committed is None:
                committed = 0
            lag = latest - committed
            print(
                f"Partition {tp.partition}: committed={committed}, latest={latest}, lag={lag}"
            )

            if lag > 0:
                offset_dict[tp] = True

        if True in list(offset_dict.values()):
            print(f"New data found for {topic}")

            return True
        else:
            return False

    except AttributeError:
        print(f"No partitions found for topic {topic}")

        return False


if __name__ == '__main__':

    args = parse_args()
    status = new_msgs_available(args.topic)
    check_pipeline_metadata("gsmls_airflow_pipeline", prop_type_=args.prop_type, key_="new_msgs", status_=status)
    sys.exit(0)

