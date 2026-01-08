import sys
from gsmls_core.gsmls.Kafka_GSMLSConsumer import KafkaGSMLSConsumer


if __name__ == "__main__":

    # Add logger back into the class script
    img_consumer = KafkaGSMLSConsumer()
    results = img_consumer.main(prop="IMAGES", logger=None, retry=False)

    if not isinstance(results, bool):
        # Need to be able to log something here
        sys.exit(1)
    else:
        # Save results in s shelf file to be shared across volumes
        sys.exit(0)