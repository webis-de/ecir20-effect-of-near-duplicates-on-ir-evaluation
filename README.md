# Content-Equivalent Documents in TREC-Tracks

## Evaluations per Track

* [Terabyte 2004](results/terabyte/2004/README.md)
  * Our reproduction of "[Redundant Documents and Search Effectiveness](https://dl.acm.org/citation.cfm?id=1099733)" by Yaniv Bernstein and Justin Zobel
  * We check our pipeline and verify that we can reproduce the results from Bernstein and Zobel (especially regarding the Mean Average Precision (MAP) local judgment manipulation)
* [Terabyte 2005]()
* [Terabyte 2006]()
* [Web 2009]()
* [Web 2010]()
* [Web 2011]()
* [Web 2012]()
* [Web 2013]()
* [Web 2014]()
* [Core 2017]()
* [Core 2018]()


## Development

We encourage pull requests and are glad to help in case of problems.

* Install the [project-lombok extension](https://projectlombok.org/) to your IDE
* Maven 3
* Java 8
* Python 3
* SPARK
* Attention: We have integration tests that are executed during the build that need access to all run-files (we execute some tests that check the integrity of our used trec-eval-params, i.e. that we get official score, hashes + counts of judged documents per shared task, etc).


### Calculate Retrieval-Equivalence-Classes

1. Create hash representations of a corpus: `make hash-dataset-<CORPUS>`
2. Deduplication: `make deduplicate-<CORPUS>`

### Calculate Content-Equivalence-Classes

1. Create a SPEX index of word 8-gramms of a corpus: `index-8-gramms-<CORPUS>`
2. Calculate S3 groups and save connected components as equivalence classes: `calculate-s3`

