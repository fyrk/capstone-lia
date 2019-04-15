"""
build model on catalog table using a given date
"""

from train_model import LibraryInterestModel
from joblib import load


if __name__ == "__main__":

    lim = load('../models/lim.joblib')
    print(lim.score())
