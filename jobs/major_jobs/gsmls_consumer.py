import argparse
import sys
import os
from gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from gsmls.utility_func import get_filepath, check_pipeline_metadata, current_status


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Data Consumption')
    parser.add_argument("--prop_type", required=True)
    parser.add_argument("--topic", required=True)

    # return parser.parse_args(['--prop_type', 'RES', '--topic', 'res_properties'])
    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()
    base_path = get_filepath('backups')
    filepath = os.path.join(base_path, f'{args.topic}.xlsx')
    retry = False
    print(f' ==== CURRENT RETRY STATUS: {retry} ====  ')

    if os.path.isfile(filepath):
        retry = True
        print(f' ==== BACKUP DATA EXISTS: {retry} ==== ')

    producer_status = current_status('gsmls_airflow_pipeline', args.prop_type, 'producer')
    consumer = KafkaGSMLSConsumer()  # Add logger back into the class script

    if producer_status is False:
        while True:
            results = consumer.main(args.prop_type, retry)

            if not isinstance(results, bool):
                # Need to be able to log something here
                retry = True
                continue
            else:
                check_pipeline_metadata("gsmls_airflow_pipeline", prop_type=args.prop_type,
                                        key="data_consumer", status=results)
                sys.exit(0)
    else:
        print(' ==== GSMLS PRODUCER ENDED PRE-MATURELY. NO NEW DATA TO BE CONSUMED ==== ')
        check_pipeline_metadata("gsmls_airflow_pipeline", prop_type=args.prop_type,
                                key="data_consumer", status=True)

