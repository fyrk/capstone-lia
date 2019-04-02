"""
Script to maintain an S3 mirror of the Seattle Public Library SODA DataLens.
Queries a JSON API and writes daily records as parquet files.
"""

import os
import sys
import logging
import json
import pickle
from time import time
from datetime import date, datetime, timedelta
from optparse import OptionParser, OptionGroup
from sodapy import Socrata
import dask.dataframe as dd


class SocrataMirror():

    def __init__(self, dataset, client, bucket):
        self.dataset = dataset
        self.client = client
        self.bucket = bucket

    def populate(self):
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

    def mirror_date(self, date):
        dump_path = 'data/interim/'
        # query SODA for single given date
        # write dataframe as parque file to S3



        # https://data.seattle.gov/resource/5src-czff.json?$where=checkoutdatetime between '2015-01-10T12:00:00' and '2015-01-10T14:00:00'
        # metadata = client.get_metadata(socrata_dataset)
        # query = f'checkoutdatetime between "{y0}" and "{y1}"'
        # query = 'id = 201903311801000010094636080'
        # results = client.get(socrata_dataset, limit=5, where=query)
        # results = client.get(socrata_dataset, limit=5, where="checkoutyear > 2018")
        # results = client.get(socrata_dataset, limit=5, where='id = 201903311801000010094636080')

        # dump raw JSON query to disk
        # dump_path = 'update_dump.json'
        # with open(dump_path, 'w') as outfile:
            # json.dump(results, outfile)

        # read JSON dump to a dask dataframe
        # df = dd.read_json(dump_path)

        # write dataframe to parquet on S3
        # df.to_parquet()

        # clean up dump_path
        logging.debug(f'Mirrored {date}.')

    def check(self):
        # check existing records
        # note date gaps (assume existing records are okay)
        gaps = []
        logging.info(f"{len(gaps)} missing dates. Last update {self.last_update}.")
        return gaps

    def repair(self):
        gaps = self.check()
        # loop through gaps and fill with mirror_date, then update
        self.update()
        logging.info('Mirror repaired.')


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
    parser.add_option_group(group)

    options, args = parser.parse_args()
    print(__doc__)
    parser.print_help()
    print()

    # display progress logs on stdout
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')

    # utility function to get the size of a dataframe when saving
    def record_size(df):
        pass

    # utility function to get bounds of a date to pass to queries
    def date_bounds(date):
        d0 = datetime.combine(date, datetime.min.time())
        d1 = datetime.combine(date, datetime.max.time())
        return d0, d1

    socrata_domain = 'data.seattle.gov'
    socrata_token = os.environ.get("SOCRATA_TOKEN")
    with Socrata(socrata_domain, socrata_token) as client:

        t0 = time() # track update time

        if options.init_mirror:
            socrata_dataset = '5src-czff'
            mirror_bucket = ''
            mirror = SocrataMirror(socrata_dataset, client, mirror_bucket)
            mirror.populate()

        else:
            with open('data/interim/mirror.pkl', 'rb') as mpkl:
                mirror = pickle.load(mpkl)
                logging.info('Mirror loaded.')

                if options.update_mirror:
                    mirror.update()

                elif options.m_date:
                    mirror.mirror_date(m_date)

                elif options.repair_mirror:
                    mirror.repair()
                
                elif options.check_mirror:
                    mirror.check()
        
        logging.info(f'Duration: {time() - t0}s.')
        with open('data/interim/mirror.pkl', 'wb') as mpkl:
            pickle.dump(mirror, mpkl)
        logging.info('Mirror pickled.')
