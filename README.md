# RecommendEngine
Implement the Item-based Recommendation Algorithm and calculate the similarity of users and items.
Implementions
=====
* Item-based Recommendation Algorithm is the most commomly used algorithm in Recommendation System, you can refer to 《Mahout in Action》Chapter1.6 “Distributing recommendation computations” for more details.
* I calculated the Item-Similarity and User-Similarity by reading paper “Empirical Analysis of Predictive Algorithms for Collaborative Filtering”， by John S.Breese.
* I also implement a CRON for RecommendEngine so that the recently-records can be fetched from MySQL to HDFS and after accomplish the computation tasks, the recommended result will be loaded into MySQL periodically. 

DataSets
==
* I got MovieLens DataSets from [here][1].

Running Environment
===
* Ubuntu 14.04
* JDK 1.7.0_75
* Hadoop 1.2.1
* Sqoop 1.3.0

Feedback
===
* Email: *tinylcy@yeah.net*  or  *tinylcy@gmail.com*
* Sina Weibo: [@tinylcy][2]


  [1]: http://grouplens.org/datasets/movielens/
  [2]: http://weibo.com/boosbossboos
