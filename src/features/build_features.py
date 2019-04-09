"""
Script to build out feature tables in RDS mirror.
Provides transform utilities as well, for updates and predicts.
"""

import os
import logging
import argparse
import psycopg2
from time import time
from tqdm import tqdm
from io import StringIO
from datetime import date, datetime, timedelta
import dask.dataframe as dd
import dask.bag as db
import daks.array as da
import pandas as pd
import numpy as np

"""
# feature tables:
# 1. checkout events: (event_id, datetime, bibnumber, callnumber)
# - no reason not to use original table
# 2. bibnumber lookup, checkout count, categoricals, and tfidf
# - bibnumber [index]
# - checkout_count (in first 6 months) [target]
# - categoricals: (type, col) -> (dummies...) [features]
# - tfidf matrices: (title, subjects) -> (bibnumber, tfidf...) [features]
"""


class CirculationFeaturizer():

    def __init__(self):
        self.conn = psycopg2.connect(
            database="mirror",
            host=os.environ.get("LIA_HOST"),
            user=os.environ.get("LIA_USER"),
            password=os.environ.get("LIA_PASSWORD"),
            port='5432')
        self.cur = self.conn.cursor()

    def init_tables(self):
        # build feature tables:
        self._init_checkouts()
        self._init_catalog()

    def _init_checkouts(self):
        # ensure clean table exists before population
        # select and extract checkout events
        # (event_id, datetime, bibnumber, callnumber)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS checkouts
            AS SELECT
                event_id,
                event_datetime,
                item_bibnum,
                item_callnum
            FROM mirror;
        """)
        self.conn.commit()

    def _init_catalog(self):
        # ensure clean table exists before population
        # select and extract item information
        # (bibnumber, checkout count, type, collection, title, subjects)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS catalog (
            AS SELECT
                item_bibnum,
                COUNT(item_bibnum) AS first_6mo_checkouts,
                MIN(event_datetime) AS first_checkout,
                item_type,
                item_collection,
                item_title,
                item_subjects
            FROM (
                SELECT
                    item_bibnum,
                    event_datetime,
                    item_type,
                    item_collection,
                    item_title,
                    item_subjects
                FROM mirror
                GROUP BY
                    item_bibnum,
                    event_datetime,
                    event_datetime,
                    item_type,
                    item_collection,
                    item_title,
                    item_subjects
                HAVING
                    event_datetime < MIN(event_datetime) + '6 months'::INTERVAL
            ) AS smf
            GROUP BY
                item_bibnum,
                item_type,
                item_collection,
                item_title,
                item_subjects
            ;""")
        self.conn.commit()

    def extract_feature_matrix(self, date_time):
        # query catalog before date_time
        # apply dummies to categoricals (type, collection)
        # extract word vectors form text (title, subjects)
        pass

    def update_tables(self):
        # recieve update records; process into feature tables
        self._update_checkouts()
        self._update_catalog()

    def _update_checkouts(self):
        pass

    def _update_catalog(self):
        pass

    def featurize_predict(self, item):
        # receive raw predict data, transform to correct format for prediction
        pass
