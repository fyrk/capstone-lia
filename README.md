# LIA

A library inventory advisor, built to assist in gauging demand for new media releases.

_Given the historical catalog usage of a library (or a private bookstore, or other similar institution), predict how popular a new release will be. How many people will want to read it? How many copies are needed?_

## Pitch & Scope

**Target audience**: Libraries trying to carefully select new pieces for their collections, to better serve their patrons. The project could also be extended to apply to any media-serving entity.

**Proposition**: Sure, you could wait until patrons ask for a book. Or wait to see a lot of inter-library requests for it. Or buy a whole bunch just to be on the safe side. But wouldn't it be better if you had a good idea of how much demand there will be for an upcoming title, and could get just the right number of copies?

**Data source**: There is lots of publicly available library catalog records. For the purposes of this project, a single library will be considered ([Seattle Public Library](https://data.seattle.gov/Community/Checkouts-By-Title-Data-Lens/5src-czff)).

**Project scope**: A 1-week MVP will entail a basic regression model predicting demand, and (possibly) a NLP tool to break down catalog items into categories based on media type and subject matter. With these components in place, a more complete recommender could be put together for the final product.

## Product

**Pipeline**: This codebase can be used as is to manually manage and update a featurized mirror of the SPL Checkouts dataset, and to train and score basic models following the sklearn train-fit schema.

## Future Work

**Automation**: Automation of the mirror maintenance and update functionality, and automated rebuilding of selected models.

**Modeling API**: A graceful API to run multiple models on the data, including clustering and recommender algorithms.

**Web Interface**: A web application to provide summary graphics of the selected models, and to power title queries and receive predictions and recommendations.
