from datetime import datetime, timedelta
import json
from collections import defaultdict
from gsmls.utility_func import create_kafka_consumer, create_mongodb_conn
from pprint import pprint
from tqdm import tqdm


def consume_data(_consumer):
    _data_list = []
    start_time = datetime.now()
    progress_bar = tqdm(range(len(_data_list)), desc='New Data Found', colour='green', position=1)

    while True:

        try:
            new_data = _consumer.poll(timeout_ms=4000)

            if new_data:
                sub_list = extract_messages(new_data)

                if isinstance(sub_list, list):
                    _data_list.extend(sub_list)
                    progress_bar.update(len(sub_list))
                else:
                    pass

            elif ((datetime.now() - start_time) < timedelta(minutes=30)) and len(_data_list) == 0:

                print(' === WAITING FOR NEW DATA === ')
                print(f' ==== CURRENT TIME LAPSE: {datetime.now() - start_time} ====')
                continue

            elif ((datetime.now() - start_time) < timedelta(minutes=30)) and len(_data_list) > 0:
                consumer.commit()
                return _data_list

            else:
                print(' === NO NEW DATA. KAFKA DATA CONSUMPTION COMPLETE === ')
                return None

        except ValueError:
            pass


def create_base_document(data: dict):

    status_data = defaultdict(str)
    float_list = ["square_feet", "lotsize_candidate_sqft", "acres_candidate_sqft", ]

    def format_value(value_):
        if isinstance(value_, dict):
            return {
                _key_.title(): format_value(_value_)
                for _key_, _value_ in value_.items()
            }

        if isinstance(value_, list):
            return [format_value(_value_) for _value_ in value_]

        return value_

    for key, value in data.items():
        formatted_key = key.title()

        if isinstance(value, (str, int)):
            status_data[formatted_key] = value

        elif isinstance(value, float):
            if key in float_list:
                status_data[formatted_key] = round(value, 2)
            else:
                status_data[formatted_key] = value

        elif isinstance(value, dict):
            sub_status_data = defaultdict(str)
            for _key, _value in value.items():
                sub_status_data[_key.title()] = format_value(_value)

            status_data[formatted_key] = dict(sub_status_data)

        elif isinstance(value, list):
            sub_status_data = defaultdict(list)
            for _value in value:
                sub_status_data[formatted_key].append(format_value(_value))

            status_data[formatted_key] = sub_status_data[formatted_key]

        else:
            status_data[formatted_key] = value

    return dict(status_data)


def extract_messages(new_messages):

    sub_list = []

    for partition_obj, messages_list in new_messages.items():

        for record in messages_list:

            try:
                _log_obj = json.loads(record.value)
                sub_list.append(_log_obj)

            except json.decoder.JSONDecodeError:

                pass
    if len(sub_list) > 0:
        return sub_list
    else:
        return None


if __name__ == '__main__':

    db_name = 'realEstate'
    collection_name = 'property_status_logs'
    consumer = create_kafka_consumer("log_consumer", "log_consumer")
    consumer.subscribe(['status_logs'])
    client = create_mongodb_conn()
    cursor = client[db_name][collection_name]
    data_list = consume_data(consumer)

    if data_list is not None:
        for log_obj in data_list:
            final_obj = create_base_document(log_obj)
            cursor.insert_one(final_obj)

