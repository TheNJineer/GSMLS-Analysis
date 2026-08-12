import argparse
import sys
import os
from gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from gsmls.utility_func import get_filepath, check_pipeline_metadata, current_status


def parse_args():

    parser = argparse.ArgumentParser(description='Consume image data from Apache Kafka into MongoDB')
    parser.add_argument("--prop_type", required=True)
    parser.add_argument("--order_num", required=True)

    # return parser.parse_args(['--prop_type', 'RES'])
    return parser.parse_args()


def parse_order_nums(num_str: str):

    order_list = num_str.split(',')
    cleaned_orders = [int(i.strip(' ')) for i in order_list]

    return cleaned_orders


if __name__ == "__main__":

    args = parse_args()
    order_nums = parse_order_nums(args.order_num)
    base_path = get_filepath('backups')
    filepath = os.path.join(base_path, f'prop_images.xlsx')
    retry = False
    print(f' ==== CURRENT RETRY STATUS: {retry} ==== ')

    if os.path.isfile(filepath):
        retry = True
        print(f' ==== BACKUP DATA EXISTS: {retry} ==== ')

    producer_status = current_status('gsmls_airflow_pipeline', args.prop_type, 'producer')
    img_consumer = KafkaGSMLSConsumer(order_nums=order_nums)  # Add logger back into the class script

    if producer_status is False:
        while True:
            results = img_consumer.main(prop="IMAGES", logger=None, retry=retry)

            if not isinstance(results, bool):
                # Need to be able to log something here
                sys.exit(1)
            else:
                check_pipeline_metadata("gsmls_airflow_pipeline", prop_type_=args.prop_type,
                                        key_="image_consumer", status_=results)
                sys.exit(0)
    else:
        print(' ==== GSMLS PRODUCER ENDED PRE-MATURELY. NO NEW DATA TO BE CONSUMED ==== ')
        check_pipeline_metadata("gsmls_airflow_pipeline", prop_type_=args.prop_type,
                                key_="image_consumer", status_=True)


