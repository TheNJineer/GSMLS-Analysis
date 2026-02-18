import os
import sys
import argparse
import shelve
from gsmls.RealEstateImages import RealEstateImages
from gsmls.utility_func import check_pipeline_metadata, cutoff_time, get_filepath


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Image Downloading')
    parser.add_argument("--local", required=True)

    # return parser.parse_args(['--local', ''])
    return parser.parse_args()


def ips_status():

    data_path = get_filepath("metadata")
    metadata_path = os.path.join(data_path, "metadata")

    with shelve.open(metadata_path) as reader:
        result = reader["gsmls_download_images"]
        print(results)
        return result["downloads_completed"]


if __name__ == "__main__":

    args = parse_args()
    program_cutoff = cutoff_time(hours=7, minutes=30, tz="America/New_York")
    status = ips_status()

    if bool(args.local) is False and status != "Expired":
        if status is not False:
            check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer")

        obj = RealEstateImages(latest_order_num=64872924)
        results = obj.download_images_main(cutoff_time=program_cutoff)
        check_pipeline_metadata("gsmls_airflow_pipeline", key="image_consumer", status=results)
        print(f" ==== TOTAL PROPERTIES QUERIED: {obj.total_props} ==== ")
        print(f" ==== TOTAL IMAGES DOWNLOADED: {obj.total_images} ==== ")
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

