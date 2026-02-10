import sys
from gsmls.utility_func import create_kafka_producer, check_pipeline_metadata


def check_kafka_connection():

    # Check if broker connects
    test_producer = create_kafka_producer(client_id='test-connection')

    if test_producer.bootstrap_connected() is True:
        print(' ==== KAFKA TEST CONNECTION WAS SUCCESSFUL ==== ')
        test_producer.close()
        return True

    else:
        return False


if __name__ == '__main__':

    status = check_kafka_connection()
    check_pipeline_metadata("gsmls_airflow_pipeline", key="kafka_connection", status=status)
    sys.exit(0)

