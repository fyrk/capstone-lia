"""
build model on catalog table using a given date
"""

import os
import logging
import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import Categorizer, DummyEncoder
from dask_ml.feature_extraction.text import HashingVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import SGDRegressor

# import numpy as np
# import dask.bag as db
# from time import time
# from datetime import date
# from datetime import datetime, timedelta
# from sparse import COO


class LibraryInterestModel():

    def _build_FT_matrix(self, model_start, model_end):
        # query catalog for raw data, index by checkout date
        host = os.environ.get("LIA_HOST")
        user = os.environ.get("LIA_USER")
        password = os.environ.get("LIA_PASSWORD")
        uri = f'postgresql://{user}:{password}@{host}:5432/mirror'
        raw_df = dd.read_sql_table('catalog', uri, 'item_bibnum')
        logging.debug("Raw dataframe pulled from catalog table.")

        # split out model range: only keep records between start and end:
        mdf = raw_df[(raw_df['first_checkout'] > model_start) &
                     (raw_df['first_checkout'] < model_end)]
        logging.debug("Dataframe clipped to model date range.")

        # extract y targets (first_6mo_checkouts)
        self._targets = mdf['first_6mo_checkouts'].to_dask_array()
        logging.debug("Targets extracted.")

        # build out onehot matrix for categoricals
        pipe = make_pipeline(Categorizer(), DummyEncoder())
        cat_x = pipe.fit_transform(mdf[['item_type', 'item_collection']])
        logging.debug("Categoricals transformed to OneHot matrix.")

        # build out tfidf matrix for text fields
        title_corpus = mdf['item_title']
        subjects_corpus = mdf['item_subjects']
        vectorizer = HashingVectorizer()
        title_x = vectorizer.transform(title_corpus)
        subjects_x = vectorizer.transform(subjects_corpus)
        logging.debug("Text fields vectorized.")

        # combine onehot (cat_x) and tfidf (title_x, subjects_x) matrices
        data = [cat_x, title_x, subjects_x]

        self._features = da.concatenate(data, axis=1,
                                        allow_unknown_chunksizes=True)
        logging.debug("Categoricals and text fields concatenated.")

    def fit(self, model_start, model_end):
        logging.info("Building features and targets...")
        self._build_FT_matrix(model_start, model_end)
        logging.info("Feature-target matrices complete.")
        logging.info("Fitting model to feature-target matrices...")
        self.clf = SGDRegressor()
        self.clf.fit(self._features.compute(), self._targets.compute())
        logging.info("Model fit complete.")

    def _transform(self, bib_item):
        transformed_bib_item = bib_item  # fake placeholder transform
        return transformed_bib_item

    def predict(self, bib_item):
        """
        * release_date could be used by build_feature_matrix to select the
        feature matrix with which to train the model.
        * the other four need transformation to align with the feature matrix.
        """
        transformed_bib_item = self._transform(bib_item)
        return self.clf.predict(transformed_bib_item)


if __name__ == "__main__":

    # display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    test_start = '2005-04-15'
    test_end = '2005-08-12'
    lim = LibraryInterestModel()

    lim.fit(test_start, test_end)
