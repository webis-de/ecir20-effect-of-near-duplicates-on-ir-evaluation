# The Effect of Content-Equivalent Near-Duplicates on the Evaluation of Search Engines

## TREC Track

* [Terabyte Track 2004](results/terabyte/2004/README.md)
  * The track originally analyzed by "[Redundant Documents and Search Effectiveness](https://dl.acm.org/citation.cfm?id=1099733)" by Yaniv Bernstein and Justin Zobel.
  * Additional reproduction of plots in Bernstein and Zobel's paper that did not fit into our paper due to space limitations.
* [Terabyte Track 2005]()
* [Terabyte Track 2006]()
* [Web Track 2009]()
* [Web Track 2010]()
* [Web Track 2011]()
* [Web Track 2012]()
* [Web Track 2013]()
* [Web Track 2014]()
* [Core Track 2017]()
* [Core Track 2018]()


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
2. Calculate S3 groups and save connected components as equivalence classes: `calculate-s3`

