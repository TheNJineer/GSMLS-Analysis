import argparse
import sys
from gsmls_core.gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Data Consumption')
    parser.add_argument("--prop_type", required=True)
    parser.add_argument("--retry", required=True)

    return parser.parse_args(['--prop_type', 'RES', '--retry', False])


if __name__ == "__main__":

    args = parse_args()
    # Add logger back into the class script
    consumer = KafkaGSMLSConsumer()
    results = consumer.main(args.prop_type, args.retry)

    if not isinstance(results, bool):
        # Need to be able to log something here
        sys.exit(1)
    else:
        # Save results in s shelf file to be shared across volumes
        sys.exit(0)

