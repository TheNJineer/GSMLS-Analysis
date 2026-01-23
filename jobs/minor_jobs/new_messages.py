import argparse
import sys
from kafka.structs import TopicPartition
from gsmls_core.gsmls.utility_func import create_kafka_consumer, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Check for new messages in TopicPartitons')
    parser.add_argument("--topic", required=True)

    return parser.parse_args(['--topic', 'res_properties'])


def new_msgs_available(topic):

    offset_dict = {}
    # KafkaConsumer not thread safe, so I need to create one specifically for this task
    cons = create_kafka_consumer(f"{topic}_msg_check", "data_consumer")

    # Check the partitions in the consumer. Returns a set of partition ids
    partitions = cons.partitions_for_topic(topic)
    print(f'{partitions}')

    try:
        if not partitions:
            print(f"No partitions found for topic {topic}")

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
    check_pipeline_metadata("gsmls_airflow_pipeline", key="new_msgs", status=status)
    # sys.exit(0)

