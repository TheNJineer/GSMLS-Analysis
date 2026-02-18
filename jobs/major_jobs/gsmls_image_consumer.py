import sys
import os
from gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from gsmls.utility_func import get_filepath, check_pipeline_metadata


if __name__ == "__main__":

    base_path = get_filepath('backups')
    filepath = os.path.join(base_path, f'prop_images.xlsx')
    retry = False
    print(f' ==== CURRENT RETRY STATUS: {retry} ==== ')

    if os.path.isfile(filepath):
        retry = True
        print(f' ==== BACKUP DATA EXISTS: {retry} ==== ')

    # Add logger back into the class script
    img_consumer = KafkaGSMLSConsumer()

    while True:
        results = img_consumer.main(prop="IMAGES", logger=None, retry=retry)

        if not isinstance(results, bool):
            # Need to be able to log something here
            sys.exit(1)
        else:
            check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer", status=results)
            sys.exit(0)

