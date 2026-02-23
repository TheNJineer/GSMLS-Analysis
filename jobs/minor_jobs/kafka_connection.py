import sys
import argparse
from gsmls.utility_func import create_kafka_producer, check_pipeline_metadata


def check_kafka_connection():

    # Check if broker connects
    test_producer = create_kafka_producer(client_id='test-connection')

    if test_producer.bootstrap_connected() is True:
        print(' ==== KAFKA TEST CONNECTION WAS SUCCESSFUL ====  ')
        test_producer.close()
        return True

    else:
        return False


def parse_args():

    parser = argparse.ArgumentParser(description="Check Apache Kafka connection status")
    parser.add_argument("--prop_type", required=True)

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    status = check_kafka_connection()
    check_pipeline_metadata("gsmls_airflow_pipeline", prop_type_=args.prop_type,
                            key_="kafka_connection", status_=status)
    sys.exit(0)

