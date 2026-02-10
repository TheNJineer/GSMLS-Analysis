import argparse
import sys
from gsmls.GSMLS import GSMLS
from gsmls.utility_func import check_pipeline_metadata, cutoff_time


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Data Production')
    parser.add_argument("--prop_type", required=True)

    return parser.parse_args(['--prop_type', 'RES'])


if __name__ == "__main__":

    args = parse_args()
    # Creates a pendulum object of the next day 2:30AM
    program_cutoff = cutoff_time(days=1, hours=2, minutes=30, tz="America/New_York")
    kwargs = {
        'prop_type': args.prop_type,
        'cutoff_time': program_cutoff
    }

    print(f' ==== CURRENT PROP TYPE: {args.prop_type}')

    obj = GSMLS(args.prop_type)
    check_pipeline_metadata("gsmls_airflow_pipeline", key="producer")

    print(f'{obj.__dict__}')
    print('==== ETL STARTED ====')
    results = obj.airflow_gsmls_producer(**kwargs)
    print('==== ETL ENDED ====')

    if not isinstance(results, int):
        # Need to be able to log something here
        print(f' === ETL FINISHED INCORRECTLY. ERROR OCCURRED SAVING INTO SQL DATABASE ==== ')
        print(results)
        sys.exit(1)
    else:
        # Save results in s shelf file to be shared across volumes
        print(f' === ETL FINISHED ==== ')
        check_pipeline_metadata("gsmls_airflow_pipeline", key="producer", status=results)
        sys.exit(0)

