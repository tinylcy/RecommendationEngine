# RecommendEngine

RecommendEngine是我本科毕业设计的部分代码，利用Hadoop并行化实现基于Item的协同过滤推荐算法和计算物品以及用户的相似度。

itemBasedRec的实现参考《Mahout in Action》中1.6 Distributing recommendation computations，这是最基本的基于物品的协同过滤推荐算法实现，基于一种假设：两个物品被同一个用户喜欢的频率越高，说明这两个物品越相似。

itemSimilarity和UserSimilarity分别计算了物品的相似度和用户的相似度，核心思想参考了John S.Breese的论文"Empirical Analysis of Predictive Algorithms for Collaborative Filtering"。





