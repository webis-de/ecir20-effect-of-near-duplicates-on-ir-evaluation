# The Effect of Content-Equivalent Near-Duplicates on the Evaluation of Search Engines

This repository contains the data and code for reproducing results of the paper:

    @InProceedings{stein:2020e,
      address =             {Berlin Heidelberg New York},
      author =              {Maik Fr{\"o}be and {Jan Philipp} Bittner and Martin Potthast and Matthias Hagen},
      booktitle =           {Advances in Information Retrieval. 42nd European Conference on IR Research (ECIR 2020)},
      doi =                 {},
      editor =              {},
      month =               apr,
      pages =               {},
      publisher =           {Springer},
      series =              {Lecture Notes in Computer Science},
      site =                {Lisbon, Portugal},
      title =               {{The Effect of Content-Equivalent Near-Duplicates on the Evaluation of Search Engines}},
      year =                2020
    }

[[Paper Link](https://webis.de/publications.html#?q=The+Effect+of+Content-Equivalent+Near-Duplicates+on+the+Evaluation+of+Search+Engines)]

## TREC Track

* [Terabyte Track 2004](results/terabyte/2004/README.md)
  * The track originally analyzed by "[Redundant Documents and Search Effectiveness](https://doi.org/10.1145/1099554.1099733)" by Yaniv Bernstein and Justin Zobel.
  * Additional reproduction of plots in Bernstein and Zobel's paper that did not fit into our paper due to space limitations.
* [Terabyte Track 2005](results/terabyte/2005/README.md)
* [Terabyte Track 2006](results/terabyte/2006/README.md)
* [Web Track 2009](results/web/2009/README.md)
* [Web Track 2010](results/web/2010/README.md)
* [Web Track 2011](results/web/2011/README.md)
* [Web Track 2012](results/web/2012/README.md)
* [Web Track 2013](results/web/2013/README.md)
* [Web Track 2014](results/web/2014/README.md)
* [Core Track 2017](results/core/2017/README.md)
* [Core Track 2018](results/core/2018/README.md)

## Aggregations over Multiple Shared Tasks

* [Global judgment manipulation](results/aggregations/README-GLOBAL.md)
* [Local judgment manipulation](results/aggregations/README-LOCAL.md)

## Usage

We encourage pull requests and are glad to help in case of problems.

* Install the [project-lombok extension](https://projectlombok.org/) to your IDE
* Maven 3
* Java 8
* Python 3
* SPARK
* Attention: We have integration tests that are executed during the build that need access to all run-files (we execute some tests that check the integrity of our used trec-eval-params, i.e. that we get official score, hashes + counts of judged documents per shared task, etc).


### Calculate retrieval equivalence classes

1. Create hash representations of a corpus: `make hash-dataset-<CORPUS>`
2. Deduplication: `make deduplicate-<CORPUS>`

### Calculate content equivalence classes

1. Create a SPEX index of word 8-gramms of a corpus: `index-8-gramms-<CORPUS>`
2. Calculate S3 groups and save connected components as equivalence classes: `calculate-s3-<CORPUS>`

