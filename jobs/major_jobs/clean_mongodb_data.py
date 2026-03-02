import argparse
from gsmls.RealEstateImages import RealEstateImages
from gsmls.utility_func import cutoff_time, check_pipeline_metadata


def parse_args():

    parser = argparse.ArgumentParser(description='Cleaning the MongoDB Database of duplicate documents')
    parser.add_argument("--local", required=True)

    # return parser.parse_args(['--local', ''])
    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(hours=4, minutes=35, tz="America/New_York")
    # program_cutoff = cutoff_time(days=1, hours=4, minutes=35, tz="America/New_York") use while debugging
    obj = RealEstateImages(latest_order_num=64872924)

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

