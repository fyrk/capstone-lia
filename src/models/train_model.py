"""
build model on catalog table using a given date range
"""

import os
import logging
import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import Categorizer

from dask_ml.feature_extraction.text import HashingVectorizer

from dask_ml.preprocessing import DummyEncoder
from dask_ml.preprocessing import OneHotEncoder

from dask_ml.linear_model import LinearRegression
from dask_ml.linear_model import PartialSGDRegressor
from dask_ml.linear_model import PartialPassiveAggressiveRegressor
from sklearn.linear_model import SGDRegressor

from dask_ml.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from datetime import date
from joblib import dump


class LibraryInterestModel():

    def __init__(self, model_end, model_start='2005-04-12'):
        # query catalog for raw data, index by checkout date
        logging.debug("Pulling raw dataframe from catalog table...")
        host = os.environ.get("LIA_HOST")
        user = os.environ.get("LIA_USER")
        password = os.environ.get("LIA_PASSWORD")
        uri = f'postgresql://{user}:{password}@{host}:5432/mirror'
        self.rdf = dd.read_sql_table('catalog', uri, 'item_bibnum')

        # split out model range: only keep records between start and end:
        logging.debug("Clipping dataframe to model date range...")
        self.m_end = model_end
        self.m_start = model_start
        self.mdf = self.rdf[(self.rdf['first_checkout'] > self.m_start) &
                            (self.rdf['first_checkout'] < self.m_end)]

        # extract y targets (first_6mo_checkouts)
        logging.debug("Extracting targets...")
        self._y = self.mdf['first_6mo_checkouts'].to_dask_array(lengths=True)

    def _build_x(self, pipe, vectorizer):
        # build out onehot matrix for categoricals
        logging.debug("Transforming categoricals to one-hot matrix...")
        cat_x = pipe.fit_transform(self.mdf[['item_type', 'item_collection']])

        # build out tfidf matrix for text fields
        logging.debug("Vectorizing text fields...")
        title_corpus = self.mdf['item_title']
        subjects_corpus = self.mdf['item_subjects']
        title_x = vectorizer.transform(title_corpus)
        subjects_x = vectorizer.transform(subjects_corpus)

        # combine onehot (cat_x) and tfidf (title_x, subjects_x) matrices
        logging.debug("Concatenating categoricals and text fields...")
        self._x = da.concatenate([cat_x, title_x, subjects_x], axis=1,
                                 allow_unknown_chunksizes=True)

    def _make_train_test_splits(self, compute=False):
        self._splits = train_test_split(self._x.compute(),
                                        self._y.compute(),
                                        test_size=0.1)

    def fit(self):
        logging.info("Fitting model to feature-target matrices...")

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

    def score(self):
        return self.clf.score(self._splits[1], self._splits[3])

    def _run_models(self, encoder, vectorizer):
        # sklearn SGDRegressor
        self.clf = SGDRegressor(max_iter=1000, tol=1e-3)
        self.clf.fit(self._splits[0].toarray(), self._splits[2])
        self.scores.append((encoder.__class__.__name__,
                            vectorizer.__class__.__name__,
                            self.clf.__class__.__name__,
                            self.clf.score(self._splits[1], self._splits[3])))
        # Dask linreg
        self.clf = LinearRegression(fit_intercept=False)
        self.clf.fit(self._splits[0].toarray(), self._splits[2])
        self.scores.append((encoder.__class__.__name__,
                            vectorizer.__class__.__name__,
                            self.clf.__class__.__name__,
                            self.clf.score(self._splits[1], self._splits[3])))
        # Dask SGDRegressor
        self.clf = PartialSGDRegressor()
        self.clf.fit(self._splits[0].toarray(), self._splits[2])
        self.scores.append((encoder.__class__.__name__,
                            vectorizer.__class__.__name__,
                            self.clf.__class__.__name__,
                            self.clf.score(self._splits[1], self._splits[3])))
        # Dask PARegressor
        self.clf = PartialPassiveAggressiveRegressor()
        self.clf.fit(self._splits[0].toarray(), self._splits[2])
        self.scores.append((encoder.__class__.__name__,
                            vectorizer.__class__.__name__,
                            self.clf.__class__.__name__,
                            self.clf.score(self._splits[1], self._splits[3])))

    def score_models(self):
        self.scores = []
        logging.info("Building features and targets...")
        encoder = OneHotEncoder()
        vectorizer = HashingVectorizer()
        pipe = make_pipeline(Categorizer(), encoder)
        self._build_x(pipe, vectorizer)
        self._make_train_test_splits()
        self._run_models(encoder, vectorizer)

        logging.info("Building features and targets...")
        encoder = DummyEncoder()
        pipe = make_pipeline(Categorizer(), encoder)
        self._build_x(pipe, vectorizer)
        self._make_train_test_splits()
        self._run_models(encoder, vectorizer)

        print(self.scores)


if __name__ == "__main__":

    # display progress logs on stdout
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')

    model_end = date.today()
    lim = LibraryInterestModel(model_end.strftime("%Y-%m-%d"))

    lim.score_models()

    # logging.info("Making train-test splits...")
    # self._make_train_test_splits()
    # lim.fit()
    # dump(lim, '../models/lim.joblib')
