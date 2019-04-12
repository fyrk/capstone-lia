# LIA

A library inventory advisor, built to assist in gauging demand for new media releases.

_Given the historical catalog usage of a library (or a private bookstore, or other similar institution), predict how popular a new release will be. How many people will want to read it? How many copies are needed?_

**Target audience**: Libraries trying to carefully select new pieces for their collections, to better serve their patrons. The project could also be extended to apply to any media-serving entity.

**Proposition**: Sure, you could wait until patrons ask for a book. Or wait to see a lot of inter-library requests for it. Or buy a whole bunch just to be on the safe side. But wouldn't it be better if you had a good idea of how much demand there will be for an upcoming title, and could get just the right number of copies?

**Data source**: There is lots of publicly available library catalog records. For the purposes of this project, a single library will be considered ([Seattle Public Library](https://data.seattle.gov/Community/Checkouts-By-Title-Data-Lens/5src-czff)).

**Project scope**: A 1-week MVP will entail a basic regression model predicting demand, and (possibly) a NLP tool to break down catalog items into categories based on media type and subject matter. With these components in place, a more complete recommender could be put together for the final product.

## Project Organization

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org

Project organization based on the [cookiecutter](https://drivendata.github.io/cookiecutter-data-science/) template.
