import sys
from gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer
from gsmls.utility_func import check_pipeline_metadata


if __name__ == "__main__":

    # Add logger back into the class script
    img_consumer = KafkaGSMLSConsumer()
    results = img_consumer.main(prop="IMAGES", logger=None, retry=False)

    if not isinstance(results, bool):
        # Need to be able to log something here
        sys.exit(1)
    else:
        check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer", status=results)
        sys.exit(0)

