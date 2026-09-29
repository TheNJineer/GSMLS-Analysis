import argparse
from gsmls.RealEstateImages import RealEstateImages
from gsmls.utility_func import cutoff_time, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Cleaning the MongoDB Database of duplicate documents')
    parser.add_argument("--local", required=True)
    parser.add_argument("--order_num", required=True)

    # return parser.parse_args(['--local', 'true'])
    return parser.parse_args()


def parse_order_nums(num_str: str):

    order_list = num_str.split(',')
    cleaned_orders = [int(i.strip(' ')) for i in order_list]

    return cleaned_orders


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(hours=4, minutes=35, tz="America/New_York")
    # program_cutoff = cutoff_time(days=1, hours=4, minutes=35, tz="America/New_York")
    order_nums = parse_order_nums(args.order_num)

    if args.local == 'false':
        obj = RealEstateImages(latest_order_num=order_nums, mongo_local=False)
    else:
        obj = RealEstateImages(latest_order_num=order_nums)

    print(' ==== CLEANING THE MONGODB ATLAS DATABASE ==== ')
    results = obj.database_cleanup(cutoff_time=program_cutoff)
    check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None,
                            key_="duplicate_clean_complete", status_=results)

