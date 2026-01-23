import os
import sys
import argparse
import shelve
from gsmls_core.gsmls.RealEstateImages import RealEstateImages
from gsmls_core.gsmls.utility_func import check_pipeline_metadata, cutoff_time, get_filepath


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Image Downloading')
    parser.add_argument("--local", required=True)

    return parser.parse_args(['--local', ''])


def ips_status():

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        result = reader["gsmls_download_images"]

        return result["downloads_completed"]


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(hours=11, minutes=41, tz="America/New_York")
    status = ips_status()

    if bool(args.local) is False and status != "Expired":
        if status is not False:
            check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer")

        results = RealEstateImages(latest_order_num=64872924).download_images_main(cutoff_time=program_cutoff)
        check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer", status=results)
        sys.exit(0)

    elif bool(args.local) is True and status != "Expired":
        # Initiates two separate objects both querying data from a MongoDB Atlas
        # and a Docker container local connection respectively
        RealEstateImages().download_images_main(cutoff_time=program_cutoff)
        RealEstateImages(local=True).download_images_main(cutoff_time=program_cutoff)
        sys.exit(0)

    else:
        print(f' ==== MORE STATIC PROXY DATA NEEDS TO BE PURCHASED. ENDING PROGRAM ==== ')
        sys.exit(0)

