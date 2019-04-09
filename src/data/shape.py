"""
Script to interface with a PostgreSQL version of the mirror on RDS.
Provides initialization and update methods.
"""

import os
import logging
from time import time
from tqdm import tqdm
import dask.dataframe as dd
import psycopg2
from io import StringIO
import pandas as pd
from mirror import load_mirror, SocrataMirror


class RDSMirrorInterface():

    def __init__(self, mirror_id):
        # "connect" to S3 mirror
        self.mirror = load_mirror(mirror_id, online=True)
        # connect to psql
        self.db = psycopg2.connect(
            database="mirror",
            host=os.environ.get("LIA_HOST"),
            user=os.environ.get("LIA_USER"),
            password=os.environ.get("LIA_PASSWORD"),
            port='5432')
        self.cur = self.db.cursor()

    # init_db retreives records from S3 mirror, and passes to write_records
    def init_db(self, date1):
        # lenrec = len(self.mirror.index)
        # logging.warning(f"Populating lia-db. {lenrec} records to process.")
        logging.warning(f"Populating lia-db.")
        # copy mirrored dates to psql
        cut_didx = [(k, v) for k, v in self.mirror.index.items() if k >= date1]
        for d_idx, mirrored in tqdm(cut_didx):
            if mirrored:
                self.write_records(self.mirror_df(d_idx))

    # Receives records from init_db or mirror update, trims,  writes to RDS.
    def write_records(self, df):
        # write dataframe to buffer in format psql can parse
        # records have commas in strings, so using \t as separator
        buff = StringIO()
        buff.write(df.to_csv(index=None, header=None, sep='\t'))
        buff.seek(0)
        # use psql bulk copy as tsv
        cc = ['event_id', 'item_bibnum', 'item_type', 'item_collection',
              'item_callnum', 'item_title', 'item_subjects', 'event_datetime']
        self.cur.copy_from(buff, 'mirror', sep='\t', columns=cc, null='None')
        self.db.commit()

    # returns a pandas dataframe with all checkout entries for given date
    def mirror_df(self, date_index):
        # look up date in mirror, load entire day's records
        date_str = f'/{date_index.year}/{date_index.month}/{date_index.day}'
        df = dd.read_parquet(self.mirror.bucket + date_str)
        # dropping redundant columns
        df = df.drop(['checkoutyear', 'itembarcode'], axis=1)
        flat_df = df.compute()
        # flat_df.dropna(inplace=True)
        return flat_df


if __name__ == "__main__":

    # display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    # initialize mirror connection
    socrata_dataset_id = '5src-czff'
    mirror_key = 'mirror.pkl'
    bucket = 's3://lia-mirror-' + socrata_dataset_id
    mirror_id = (socrata_dataset_id, bucket, mirror_key)
    logging.info("Connected to dask/parquet mirror on S3.")

    # initialize db connection
    dbi = RDSMirrorInterface(mirror_id)
    logging.info("Connected to PostgresDB on RDS.")

    # initialize entire db
    t0 = time()  # track process time
    dbi.init_db()
    logging.info(f'Duration: {time() - t0}s.')
