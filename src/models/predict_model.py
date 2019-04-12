"""
build model on catalog table using a given date
"""

from train_model import LibraryInterestModel


if __name__ == "__main__":

    test_start = '2005-04-15'
    test_end = '2005-08-12'
    lim = LibraryInterestModel()

    lim.fit(test_start, test_end)
