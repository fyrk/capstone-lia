"""
build model on catalog table using a given date
"""

import os
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
        raw_df = dd.read_sql_table('catalog', uri, 'first_checkout')

        # split out model range: only keep records between start and end:

        # throws KeyError: 1113523200000000000
        # mdf = raw_df.loc[model_start:model_end]

        # throws DateParseError:
        # Unknown datetime string format, unable to parse: first_checkout
        mdf = raw_df[(raw_df['first_checkout'] > model_start) &
                     (raw_df['first_checkout'] < model_end)]

        # extract y targets (first_6mo_checkouts)
        self._targets = mdf['first_6mo_checkouts'].to_dask_array()

        # build out onehot matrix for categoricals
        pipe = make_pipeline(Categorizer(), DummyEncoder())
        cat_x = pipe.fit_transform(mdf[['item_type', 'item_collection']])  # da

        # build out tfidf matrix for text fields
        title_corpus = mdf['item_title']
        subjects_corpus = mdf['item_subjects']
        vectorizer = HashingVectorizer()
        title_x = vectorizer.transform(title_corpus)  # da
        subjects_x = vectorizer.transform(subjects_corpus)  # da

        # combine onehot (cat_x) and tfidf (title_x, subjects_x) matrices
        data = [cat_x, title_x, subjects_x]

        # throws TypeError: Truth of Delayed objects is not supported
        self._features = da.concatenate(data, axis=1)

    def fit(self, model_start, model_end):
        self._build_FT_matrix(model_start, model_end)
        self.clf = SGDRegressor()
        self.clf.fit(self._features, self._targets)

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

    test_start = '2005-04-15'
    test_end = '2005-08-12'
    lim = LibraryInterestModel()

    lim.fit(test_start, test_end)
