# RecommendEngine

RecommendEngine是我本科毕业设计的部分代码，利用Hadoop并行化实现基于Item的协同过滤推荐算法和计算物品以及用户的相似度。

itemBasedRec的实现参考《Mahout in Action》中1.6 Distributing recommendation computations，这是最基本的基于物品的协同过滤推荐算法实现，基于一种假设：两个物品被同一个用户喜欢的频率越高，说明这两个物品越相似。

itemSimilarity和UserSimilarity分别计算了物品的相似度和用户的相似度，核心思想参考了John S.Breese的论文"Empirical Analysis of Predictive Algorithms for Collaborative Filtering"。

GroupLens数据集：http://www.grouplens.org/node/12

基于以上我实现了一个电影推荐系统，关系型数据库采用MySQL，服务器采用Apache Tomcat,推荐系统的核心功能为大众化电影推荐、个性化电影推荐、相似物品推荐、相似用户推荐。推荐引擎部分为基于Item的协同过滤推荐算法和基于User的协同过滤推荐算法混合的推荐引擎组，其中基于Item的协同过滤推荐算法通过itemBasedRec实现，作为离线计算部分(同时包括itemSimilarity和UserSimilarity的计算)，基于User的协同过滤推荐算法通过Mahout实现，作为在线计算部分。

为了保证推荐系统可以周期性导入用户新产生的行为记录，利用Sqoop实现系统定时器。











