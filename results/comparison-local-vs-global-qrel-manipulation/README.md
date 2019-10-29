# Comparison of Local and Global Manipulation of Judgments under the Novelty Principle

The script [eval.sh](eval.sh) shows the different behavior of local and global manipulation in the case that a ranking does not retrieve any document of a relevant class of content-equivalent documents. The script produces the following output:


[Ranking one](ranking_one_original) has a class of relevant, content-equivalent documents on positions three, four, and five. [Ranking two](ranking_two_original) has a relevant document on position two. Ranking one is superior in terms of MAP on the original judgments.

```
RankingOne: map                   	all	0.3583
RankingTwo: map                   	all	0.1250
```

Local judgment manipulation causes that ranking one is missing one document, while ranking two is missing three documents. The result is that Ranking one is superiour Ranking two. However, Ranking two should receive the larger score, since both rankings find one relevant document, but ranking two on a higher position!

```
RankingOne: map                   	all	0.1667
RankingTwo: map                   	all	0.1250
```

This effect is removed if we apply the global-manipulation:

```
RankingOne: map                   	all	0.1667
RankingTwo: map                   	all	0.2500
```

