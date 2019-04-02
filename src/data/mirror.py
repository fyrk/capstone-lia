"""
Script to maintain an S3 mirror of the Seattle Public Library SODA DataLens.
Queries a JSON API and writes daily records as parquet files.
"""

import os
import sys
import logging
import pickle
import boto3
import pandas as pd
import numpy as np
from time import time
from datetime import date, datetime, timedelta
from optparse import OptionParser, OptionGroup
from sodapy import Socrata
import dask.bag as db
import dask.dataframe as dd


class SocrataMirror():

    def __init__(self, dataset, client, bucket):
        self.dataset = dataset
        self.client = client
        self.bucket = bucket

    def populate(self, online=False):
        # set self.last_update to be day before first day in dataset
        self.last_update = 'an ancient time'
        # update entire dataset
        # self.update()
        logging.info('Mirror initialized.')

    def update(self):
        # CAREFUL: DON'T 'MALICIOUSLY' HAMMER SOCRATA
        delay = 33
        # create daterange
        # loop from last update through yesterday
        for date in daterange:
            # call mirror_date for each date
            time.sleep(delay)
        logging.info(f"Mirror updated with {len(daterange)} records.")
        # refresh last_update marker
        self.last_update = date.today() - timedelta(days=1)

    def mirror_date(self, date, local=False):

        # query SODA for single given date, and gather into dask bag
        d0, d1 = date_bounds(date)
        query = f"checkoutdatetime between '{d0}' and '{d1}'"
        results = self.client.get(self.dataset, limit=5, where=query)
        bagged_results = db.from_sequence(results)

        # clarify metadata and flatten bag into dataframe
        meta = pd.DataFrame(columns=['id', 'checkoutyear', 'bibnumber',
                                     'itembarcode', 'itemtype', 'collection',
                                     'callnumber', 'itemtitle', 'subjects',
                                     'checkoutdatetime'])
        meta.checkoutyear = meta.checkoutyear.astype(np.int64)
        meta.bibnumber = meta.bibnumber.astype(np.int64)
        meta.checkoutdatetime = meta.checkoutdatetime.astype(np.datetime64)
        date_frame = bagged_results.to_dataframe()

        # write dataframe to single parquet file on S3
        date_frame = date_frame.repartition(npartitions=1)
        dd.to_parquet(date_frame, f'{self.bucket}/records', append=True)

    def check(self):
        # check existing records
        # note date gaps (assume existing records are okay)
        gaps = []
        summary = f"{len(gaps)} missing. Last update: {self.last_update}."
        logging.info(summary)
        return gaps

    def repair(self):
        gaps = self.check()
        # loop through gaps and fill with mirror_date, then update
        self.update()
        logging.info('Mirror repaired.')


# utility function to get bounds of a date to pass to queries
def date_bounds(date):
    d0 = datetime.combine(date, datetime.min.time())
    d0f = d0.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    d1 = datetime.combine(date, datetime.max.time())
    d1f = d1.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    return d0f, d1f

def load_mirror(mirror_id, online=False, client=None):
    # unpack id variables
    socrata_id, bucket, mirror_key = mirror_id
    if client:
        mirror = SocrataMirror(socrata_id, client, bucket)
    else:
        if online:
            s3_client = boto3.resource('s3')
            mirror_pkl = s3_client.Object(bucket[5:], mirror_key)
            pbo = mirror_pkl.get()['Body'].read()
            mirror = pickle.loads(pbo)
            logging.info('Mirror unpickled from S3.')
        else:
            mirror = pickle.load(open(bucket+mirror_key, 'rb'))
            logging.info(f"Mirror unpickled from {bucket}.")
    return mirror

def freeze_mirror(mirror_id, online=False):
    # unpack id variables
    _, bucket, mirror_key = mirror_id
    if online:
        s3_client = boto3.resource('s3')
        pbo = pickle.dumps(mirror)
        s3_client.Object(bucket[5:], mirror_key).put(Body=pbo)
    else:
        pickle.dump(mirror, open(bucket+mirror_key, 'wb'))
    logging.info(f"Mirror pickled to {bucket}.")


if __name__ == "__main__":

    # parse commandline arguments
    parser = OptionParser()
    parser.add_option("-i", "--init",
                      action="store_true", dest="init_mirror", default=False,
                      help="Initialize new mirror with all existing records.\
                      This can take days.")
    parser.add_option("-u", "--update",
                      action="store_true", dest="update_mirror", default=False,
                      help="Update existing mirror with any new records.")
    parser.add_option("-c", "--check",
                      action="store_true", dest="check_mirror", default=False,
                      help="Verifies mirror, listing gaps and last update.")
    parser.add_option("-r", "--repair",
                      action="store_true", dest="repair_mirror", default=False,
                      help="Fills gaps and updates mirror.")

    group = OptionGroup(parser, "Debug options")
    group.add_option("-d", "--date",
                      action="store", type="string", dest="m_date",
                      help="Selectively mirror a specific date.\
                      Specify date with format YYYY-MM-DD.")
    group.add_option("-o", "--online",
                     action="store_true", dest="online", default=False,
                     help="Set to run on S3. Default is offline for testing.")
    parser.add_option_group(group)

    options, args = parser.parse_args()
    print(__doc__)
    parser.print_help()
    print()

    # display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    socrata_domain = 'data.seattle.gov'
    socrata_token = os.environ.get("SOCRATA_TOKEN")
    with Socrata(socrata_domain, socrata_token) as client:

        t0 = time() # track process time

        socrata_dataset_id = '5src-czff'
        mirror_key ='mirror.pkl'
        bucket = 's3://lia-mirror-' + socrata_dataset_id if options.online\
                                                 else 'data/interim/'
        mirror_id = (socrata_dataset_id, bucket, mirror_key)

        if options.init_mirror:
            mirror = load_mirror(mirror_id, options.online, client=client)
            mirror.populate()
            freeze_mirror(mirror_id, options.online)
        elif options.update_mirror:
            mirror = load_mirror(mirror_id, options.online)
            mirror.update()
            freeze_mirror(mirror_id, options.online)
        elif options.repair_mirror:
            mirror = load_mirror(mirror_id, options.online)
            mirror.repair()
            freeze_mirror(mirror_id, options.online)
        elif options.check_mirror:
            mirror = load_mirror(mirror_id, options.online)
            mirror.check()
        elif options.m_date:
            mirror = load_mirror(mirror_id, options.online)
            mirror.mirror_date(datetime.strptime(options.m_date, '%Y-%m-%d'))
            logging.info(f'Mirrored {options.m_date}.')

        logging.info(f'Duration: {time() - t0}s.')
