"""
Script to maintain an S3 mirror of the Seattle Public Library SODA DataLens.

"""

from __future__ import print_function

import os
import sys
import logging
import json
from time import time
from datetime import date, datetime, timedelta
from optparse import OptionParser

from sodapy import Socrata

import dask.dataframe as dd


# Display progress logs on stdout
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')


# parse commandline arguments
parser = OptionParser()
parser.add_option("-i", "--init",
                  action="store_true", dest="init_mirror", default=False,
                  help="Initialize a new mirror with all records.")
parser.add_option("-u", "--update",
                  action="store_true", dest="update_mirror", default=False,
                  help="Update an existing mirror with new records.")
parser.add_option("-d", "--date",
                  action="store", type="string", dest="mirror_date",
                  help="Mirror a specific date.")
parser.add_option("-c", "--check",
                  action="store_true", dest="check_mirror", default=True,
                  help="Check mirror, noting the last update, and any gaps.")
parser.add_option("-r", "--repair",
                  action="store_true", dest="repair_mirror", default=False,
                  help="Check mirror, filling any gaps.")

(options, args) = parser.parse_args()
print(__doc__)
parser.print_help()
print()

# def size_mb(docs):
    # return sum(len(s.encode('utf-8')) for s in docs) / 1e6

socrata_domain = 'data.seattle.gov'
socrata_token = os.environ.get("SOCRATA_TOKEN")
socrata_dataset = '5src-czff'

with Socrata(socrata_domain, socrata_token) as client:

    if options.init_mirror:
        # get current date
        # parallelize SODA queries to create data frames from days up through yesterday
        # write dataframes as parquet files to S3
        pass

    elif options.update_mirror:

        # get yesterday's date
        yesterday = date.today() - timedelta(days=1)
        y0 = datetime.combine(yesterday, datetime.min.time())
        y1 = datetime.combine(yesterday, datetime.max.time())

        # track update time
        t0 = time()

        # dump raw JSON from SODA query for yesterday
        # metadata = client.get_metadata(socrata_dataset)
        # query = f'checkoutdatetime between "{y0}" and "{y1}"'
        query = 'id = 201903311801000010094636080'
        # results = client.get(socrata_dataset, limit=5, where=query)
        results = client.get(socrata_dataset, limit=5, where="checkoutyear > 2018")
        results = client.get(socrata_dataset, limit=5, where='id = 201903311801000010094636080')
        dump_path = 'update_dump.json'
        with open(dump_path, 'w') as outfile:
            json.dump(results, outfile)

        # https://data.seattle.gov/resource/5src-czff.json?$where=checkoutdatetime between '2015-01-10T12:00:00' and '2015-01-10T14:00:00'

        # write dataframe as parquet file to S3
        df = dd.read_json(dump_path)
        # df.to_parquet()
        duration = time() - t0

        print("Update completed in %fs:" % duration, end=' ')
        print("processed %d new records" % len(df.index), end='')
        # print("(%0.3fMB)." % size_mb(df))
        print()

    elif options.mirror_date:
        # SODA query for given date
        # write dataframe as parque file to S3
        pass

    elif options.repair_mirror:
        # Get results of check_mirror
        # Soda query gaps and recent missing records, write to S3
        pass

    if options.check_mirror:
        # check existing records
        # note date gaps (assume existing records are okay)
        # note last date
        # return dict
        pass
