import argparse
import sys
import pendulum
from datetime import time as dtime
from datetime import timedelta
from pendulum import timezone
from gsmls_core.gsmls.GSMLS import GSMLS


def cutoff_conversion(cutoff_time_: dtime, tz_):

    now = pendulum.now(tz_)
    next_day = now + timedelta(days=1)

    cutoff_dt = next_day.replace(
        hour=cutoff_time_.hour,
        minute=cutoff_time_.minute
    )

    print(f" ==== THE CUTOFF TIME IS : {cutoff_dt} ==== ")
    return cutoff_dt


def parse_args():

    parser = argparse.ArgumentParser(description='GSMLS Data Production')
    parser.add_argument("--prop_type", required=True)

    return parser.parse_args(['--prop_type', 'RES'])


if __name__ == "__main__":

    args = parse_args()
    tz = timezone("America/New_York")
    cutoff = dtime(hour=2, minute=30, tzinfo=tz)
    cutoff_time = cutoff_conversion(cutoff, tz)  # Creates a pendulum object of the next day 2:30AM
    kwargs = {
        'prop_type': args.prop_type,
        'cutoff_time': cutoff_time
    }

    print(f'This is the prop type: {args.prop_type}')
    print(f'This is the cutoff time: {cutoff_time}')

    obj = GSMLS(args.prop_type)

    print(f'{obj.__dict__}')
    print('==== ETL STARTED ====')
    results = obj.airflow_gsmls_producer(**kwargs)
    print('==== ETL ENDED ====')

    if not isinstance(results, int):
        # Need to be able to log something here
        print(f' === JOB FINISHED INCORECCTLY ==== ')
        # sys.exit(1)
    else:
        # Save results in s shelf file to be shared across volumes
        print(f' === JOB FINISHED ==== ')
        # sys.exit(0)