"""
Script to maintain an S3 mirror of the Seattle Public Library SODA DataLens.
Queries a JSON API and writes daily records as parquet files.
"""

import os
import logging
import pickle
import boto3
import pandas as pd
import numpy as np
from time import time, sleep
from tqdm import tqdm
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
        self.index = {}
        logging.info('Building mirror index...')
        self.last_update = date(2005, 4, 12)  # all records follow this date
        self.extend_index()
        logging.info('Mirror initialized.')

    def extend_index(self):
        for idx in date_index(self.last_update):
            self.index[idx] = False

    def repair_index(self):
        self.last_update = date(2005, 4, 12)
        self.extend_index()
        for idx in tqdm(self.index.keys()):
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(
                Bucket=self.bucket[5:],
                Prefix=f'{idx.year}/{idx.month}/{idx.day}')
            self.index[idx] = response
        logging.info('Repaired mirror index.')
        self.check()

    def update(self, limit):
        # CAUTION: LONG LOOP! Don't 'maliciously' hammer the Socrata API.
        delay = 30  # chosen to allow a ~48hr mirror as of April 2019.
        # create list of gap jobs, loop through it
        # extend index
        self.extend_index()
        records = self.jobs(limit) if limit > 0 else self.jobs()
        logging.warning(f"Hold on, {len(records)} records to mirror...")
        for i, idx in enumerate(tqdm(records)):
            self.mirror_date(idx)  # call mirror_date for each date
            self.index[idx] = True  # mark date as mirrored
            length = len(records)
            if (length > 1) and (i < length - 1):
                logging.debug(f"Sleeping for {delay}s...")
                sleep(delay)
        self.last_update = yesterday()
        logging.info(f"Mirror updated with {len(records)} records.")

    def mirror_date(self, date, local=False):
        # query SODA for single given date, and gather into dask bag
        # SODA returns 1000 records per get by default
        # set limit above default with caution
        # alternate method: page query with offset (not implemented here)
        d0, d1 = date_bounds(date)
        query = f"checkoutdatetime between '{d0}' and '{d1}'"
        logging.debug(f"Querying Socrata...")
        bagged_results = db.from_sequence(self.client.get(self.dataset,
                                                          limit=100000,
                                                          where=query),
                                          npartitions=24)
        logging.debug(f"Query complete. Framing...")

        # clarify metadata and flatten bag into dataframe
        meta = pd.DataFrame(columns=['id', 'checkoutyear', 'bibnumber',
                                     'itembarcode', 'itemtype', 'collection',
                                     'callnumber', 'itemtitle', 'subjects',
                                     'checkoutdatetime'])
        meta.bibnumber = meta.bibnumber.astype(np.int64)
        meta.checkoutyear = meta.checkoutyear.astype(np.int64)
        meta.checkoutdatetime = meta.checkoutdatetime.astype(np.datetime64)

        date_frame = bagged_results.to_dataframe(meta=meta)
        logging.debug(f"Framing complete. Writing...")

        # write dataframe to parquet file on S3
        dd.to_parquet(date_frame,
                      f'{self.bucket}/{date.year}/{date.month}/{date.day}')
        logging.debug(f"Task 'Write Mirror {date}': complete.")

    def jobs(self, limit=None):
        # mark empties
        jobs = []
        for idx, mirrored in self.index.items():
            if not mirrored:
                jobs.append(idx)
        return jobs[:limit] if limit else jobs

    def check(self):
        # check existing records
        # note date jobs (assume existing records are okay)
        current_length = len(self.index)
        entries = sum(1 for x in self.index.values() if x)
        last_update = self.last_update
        status = "up to" if last_update == yesterday() else "out of"
        new_count = len(self.jobs())

        summary = f"Current mirror is {entries/current_length:2f}% complete.\n\
                    \t     Mirror contains {entries} entries.\n\
                    \t     Index {status} date (last update: {last_update}).\n\
                    \t     There are {new_count} gaps that can be mirrored."
        logging.info(summary)


def yesterday():
    return date.today() - timedelta(days=1)


def date_index(start_mark, end=yesterday()):
    dates = []
    idx = start_mark
    while idx < end:
        idx += timedelta(days=1)
        dates.append(idx)
    return dates


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
            mirror = pickle.load(open(bucket + mirror_key, 'rb'))
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
        pickle.dump(mirror, open(bucket + mirror_key, 'wb'))
    logging.info(f"Mirror pickled to {bucket}.")


if __name__ == "__main__":

    # parse commandline arguments
    parser = OptionParser()
    parser.add_option("-i", "--init",
                      action="store_true", dest="init_mirror", default=False,
                      help="Initialize new mirror with connections.")
    parser.add_option("-u", "--update",
                      action="store", type="int", dest="update_limit",
                      help="Update mirror with all missing records.")
    parser.add_option("-c", "--check",
                      action="store_true", dest="check_mirror", default=False,
                      help="Verifies mirror, lists # of jobs.")

    group = OptionGroup(parser, "Debug options")
    group.add_option("-d", "--date",
                     action="store", type="string", dest="m_date",
                     help="Selectively mirror a specific date.\
                     Specify date with format YYYY-MM-DD.")
    group.add_option("-o", "--online",
                     action="store_true", dest="online", default=False,
                     help="Set to run on S3. Default is offline for testing.")
    group.add_option("-r", "--repair",
                     action="store_true", dest="repair", default=False,
                     help="Set to repair index by checking S3 contents.")
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

        t0 = time()  # track process time

        socrata_dataset_id = '5src-czff'
        mirror_key = 'mirror.pkl'
        bucket = 's3://lia-mirror-' + socrata_dataset_id if options.online\
            else 'data/interim/'
        mirror_id = (socrata_dataset_id, bucket, mirror_key)

        if options.init_mirror:
            mirror = load_mirror(mirror_id, options.online, client=client)
            freeze_mirror(mirror_id, options.online)
        elif options.update_limit:
            mirror = load_mirror(mirror_id, options.online)
            mirror.update(options.update_limit)
            freeze_mirror(mirror_id, options.online)
        elif options.check_mirror:
            mirror = load_mirror(mirror_id, options.online)
            mirror.check()
        elif options.m_date:
            mirror = load_mirror(mirror_id, options.online)
            mirror.mirror_date(datetime.strptime(options.m_date, '%Y-%m-%d'))
            logging.info(f'Mirrored {options.m_date}.')
        elif options.repair:
            mirror = load_mirror(mirror_id, options.online)
            mirror.repair_index()

        logging.info(f'Duration: {time() - t0}s.')
