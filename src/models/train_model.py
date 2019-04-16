"""
build model on catalog table using a given date range
"""

import os
import logging
import dask.dataframe as dd
from datetime import date
# from joblib import dump

from dask_ml.model_selection import train_test_split
from dask_ml.preprocessing import Categorizer, DummyEncoder, OneHotEncoder
from dask_ml.feature_extraction.text import HashingVectorizer
from dask_ml.wrappers import Incremental
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import SGDRegressor, PassiveAggressiveRegressor


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
        cx_df = pipe.fit_transform(self.mdf[['item_type', 'item_collection']])
        self._x = cx_df.to_dask_array(lengths=True)

    def fit(self, clf):
        logging.info("Fitting selected model...")
        self.clf = Incremental(clf)
        self.clf.fit(self._x, self._y)

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

    def _score(self, vec, enc, clf, xtr, ytr, xts, yts):
        model = Incremental(clf)
        model.fit(xtr, ytr)
        logging.info(f"""Results:
                        {enc.__class__.__name__}
                        {vec.__class__.__name__}
                        {clf.__class__.__name__}
                        R^2: {model.score(xts, yts)}""")

    def _scm(self, enc, vec):
        logging.info("Preparing models...")
        pipe = make_pipeline(Categorizer(), enc)
        self._build_x(pipe, vec)
        xtr, xts, ytr, yts = train_test_split(self._x, self._y, test_size=0.1)
        # Dask Incremental; sklearn SGDRegressor
        logging.debug("...with incremental SGDRegressor")
        self._score(vec, enc, SGDRegressor(), xtr, ytr, xts, yts)
        # Dask Incremental; sklearn PassiveAggressiveRegressor
        logging.debug("...with incremental PassiveAggressiveRegressor")
        self._score(vec, enc, PassiveAggressiveRegressor(), xtr, ytr, xts, yts)

    def score_models(self):
        logging.info("Scoring OneHotEncoder...")
        self._scm(OneHotEncoder(), HashingVectorizer())
        logging.info("Scoring DummyEncoder...")
        self._scm(DummyEncoder(), HashingVectorizer())


if __name__ == "__main__":

    # display progress logs on stdout
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')

    # model_end = date.today()
    model_end = date(2005, 6, 2)
    lim = LibraryInterestModel(model_end.strftime("%Y-%m-%d"))

    lim.score_models()

    # lim.fit()
    # dump(lim, '../models/lim.joblib')
