import argparse
from gsmls.RealEstateImages import RealEstateImages
from gsmls.utility_func import cutoff_time, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Cleaning the MongoDB Database of duplicate documents')
    parser.add_argument("--local", required=True)
    parser.add_argument("--order_num", required=True)

    # return parser.parse_args(['--local', 'false'])
    return parser.parse_args()


def parse_order_nums(num_str: str):

    order_list = num_str.split(',')
    cleaned_orders = [int(i.strip(' ')) for i in order_list]

    return cleaned_orders


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(hours=4, minutes=35, tz="America/New_York")
    # program_cutoff = cutoff_time(days=1, hours=4, minutes=35, tz="America/New_York")   use while debugging
    order_nums = parse_order_nums(args.order_num)
    obj = RealEstateImages(latest_order_num=order_nums)

    if args.local == 'false':
        print(' ==== CLEANING THE MONGODB ATLAS DATABASE ==== ')
        results = obj.database_cleanup(cutoff_time=program_cutoff)
        check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None,
                                key_="duplicate_clean_complete", status_=results)
    else:
        # Initiates two separate objects both querying data from a MongoDB Atlas
        # and a Docker container local connection respectively
        print(' ==== CLEANING THE MONGODB ATLAS & DOCKER DATABASE ====  ')
        results = obj.database_cleanup(cutoff_time=program_cutoff)
        results2 = RealEstateImages(latest_order_num=64872924, local=True).database_cleanup(cutoff_time=program_cutoff)
        check_pipeline_metadata("gsmls_cleaning_pipeline", prop_type_=None,
                                key_="duplicate_clean_complete", status_=(results, results2))

