"""
Script to interface with a Postgres version of the mirror on RDS.
Provides initialization and update methods.
"""

import os
import logging
from time import time
from tqdm import tqdm
import dask.dataframe as dd
import psycopg2
from src.data.mirror import load_mirror


class RDSMirrorInterface():

    def __init__(self):
        # connect to RDS
        self.db = psycopg2.connect(
            database="mirror",
            host=os.environ.get("LIA_HOST"),
            user=os.environ.get("LIA_USER"),
            password=os.environ.get("LIA_PASSWORD"),
            port='5432')
        self.cur = self.db.cursor()
        self.schema = "(event_id, event_datetime,\
                        item_bibnum, item_title,\
                        item_callnum, item_type,\
                        item_collection, item_subjects)"

    # init_db retreives records from S3 mirror, and passes to write_records
    def init_db(self, bucket_name, mirror_index):
        # loop through mirror records (in parallel?) (looping BAD!)
        # >>> FIND ANOTHER WAY <<<
        # >>> USE COPY <<<
        logging.warning(f"Populating lia-db.\
            {len(mirror_index.keys())} records to process.")
        for date_idx, mirrored in tqdm(mirror_index):
            if mirrored:
                self.write_records(self.mirror_df(bucket_name, date_idx))

    # Receives records from init_db or mirror update, trims,  writes to RDS.
    def write_records(self, df):
        # get trimmed_df to self.db
        trimmed_df = self.trim(df)
        for row in trimmed_df:
            values = f"(\
                {trimmed_df.ID}, {trimmed_df.CheckoutDateTime},\
                {trimmed_df.BibNumber}, {trimmed_df.ItemTitle},\
                {trimmed_df.CallNumber}, {trimmed_df.ItemType},\
                {trimmed_df.Collection}, {trimmed_df.Subjects})"
            # populate query string
            query = f"INSERT INTO mirror {self.schema} VALUES {values};"
            # queue write row to db
            self.cur.execute(query)
        # commit writes
        self.db.commit()

    def mirror_df(self, bucket_name, date_index):
        # look up date in mirror
        df = dd.read_parquet(f'{bucket_name}/\
            {date_index.year}/{date_index.month}/{date_index.day}')
        return df

    # >>> better to just copy raw, and transform in Postgres?
    def trim(self, df):
        trimmed_df = df.drop(['CheckoutYear', 'ItemBarcode'], axis=1)
        return trimmed_df.compute()


if __name__ == "__main__":

    # display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    # initialize mirror connection
    socrata_dataset_id = '5src-czff'
    mirror_key = 'mirror.pkl'
    bucket = 's3://lia-mirror-' + socrata_dataset_id
    mirror_id = (socrata_dataset_id, bucket, mirror_key)
    mirror = load_mirror(mirror_id, online=True)
    logging.info("Connected to dask/parquet mirror on S3.")

    # initialize db connection
    dbi = RDSMirrorInterface()
    logging.info("Connected to PostgresDB on RDS.")

    # initialize entire db
    t0 = time()  # track process time
    dbi.init_db(bucket, mirror.index)
    logging.info(f'Duration: {time() - t0}s.')
