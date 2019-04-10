"""
train model on catalog table using a given date
"""

import os
import numpy as np
import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import Categorizer, DummyEncoder
from dask_ml.feature_extraction import HashingVectorizer
from sparse import COO
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import SGDRegressor

from time import time
from datetime import date, datetime, timedelta


class LibraryInterestModel():

    def __init__(self, model_start, model_end):
        self.start = model_start
        self.end = model_end

    def _build_feature_matrix(self):
        # query catalog for raw data
        host = os.environ.get("LIA_HOST"),
        user = os.environ.get("LIA_USER"),
        password = os.environ.get("LIA_PASSWORD"),
        uri = f'postgresql://{user}:{password}@{host}:5432/mirror'
        df = dd.read_sql_table('catalog', uri, 'item_bibnum')

        # build out onehot matrix for categoricals
        catX = df[['item_type', 'item_collection']]
        pipe = make_pipeline(
            Categorizer(),
            DummyEncoder()
        )
        catXa = catX.to_dask_array()
        self._cat_enc = OneHotEncoder(sparse=True).fit(catXa)
        result = self.cat_enc.transform(catXa)
        result.map_blocks(COO.from_scipy_sparse, dtype=result.dtype).compute()

        # build out tfidf matrix for text fields
        textX = df[['item_title', 'item_subjects']]

        self._targets = df.first_6mo_checkouts
        self._features = df.array()

    def _transform(self, bib_item):

        transformed_item = bib_item
        return transformed_item

    def fit(self):
        self._build_feature_matrix()
        self.clf = SGDRegressor()
        self.clf.fit(self.fm, self.targets)
        pass

    def predict(self, bib_item):
        """bib_item is a 1-row dataframe with columns:
            - release_date (datetime of first checkout),
            - type,
            - collection,
            - title,
            - subjects
        * release_date is used by build_feature_matrix to select the feature
        matrix with which to train the model.
        * the other four need transformation to align with the feature matrix.
        """
        tbi = self._transform(bib_item)
        return self.clf.predict(tbi)
