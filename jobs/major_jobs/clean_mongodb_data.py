import argparse
from gsmls_core.gsmls.RealEstateImages import RealEstateImages
from gsmls_core.gsmls.utility_func import check_pipeline_metadata, cutoff_time


def parse_args():

    parser = argparse.ArgumentParser(description='Cleaning the MongoDB Database of duplicate documents')
    parser.add_argument("--local", required=True)

    return parser.parse_args(['--local', ''])


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(minutes=41, tz="America/New_York")

    if bool(args.local) is False:
        print(' ==== CLEANING THE MONGODB ATLAS DATABASE ==== ')
        RealEstateImages().database_cleanup(cutoff_time=program_cutoff)
    else:
        # Initiates two separate objects both querying data from a MongoDB Atlas
        # and a Docker container local connection respectively
        print(' ==== CLEANING THE MONGODB ATLAS & DOCKER DATABASE ==== ')
        RealEstateImages().database_cleanup(cutoff_time=program_cutoff)
        RealEstateImages(local=True).database_cleanup(cutoff_time=program_cutoff)

