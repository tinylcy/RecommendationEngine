## RecommendationEngine

The key of Recommendation Engine is an efficient and scalable implementation of item-based collaborative filtering (CF) recommendation algorithm based on Hadoop.

### Purpose

Item-based CF algorithm has become one of the most popular algorithms in recommendation systems. However, the item-based CF algorithm has been traditionally run in stand-alone mode and can be hindered by some hardware constraints, such as memory and computational limitations. Besides, in recent years recommendation systems are usually required to process large volumes of information with high dimensions, which poses some key challenges to provide recommendations quickly. So despite some excellent algorithms like item based CF running well in stand-alone mode, there is an impracticality in the condition of huge amount of users and items. This is the scalability problem and whether it can be solved properly determines the further development of recommendation systems.

### Algorithms

The similarity between items is integrated with empirical analysis.

![image](https://github.com/tinylcy/RecommendationEngine/raw/master/data/img/similarity.png)

Once the similarity between items have been calculated, the next step is computing the predicted rating of user *u* to item *j* , which is represented as follows.

![image](https://github.com/tinylcy/RecommendationEngine/raw/master/data/img/prediction.png)

### Run

```shell
nohup hadoop jar RecommendationEngine.jar -filename filename -reducer reducer -n n -m m -p p -q q -r r 
-host host -path path -mode mode >logfile 2>&1 &
```

* **filename** : input file
* **reducer** : the number of reducer.
* **n** : the number of items
* **m** : the number of users
* **p** : matrix block size
* **q** : matrix block size
* **r** : matrix block size
* **host** : JobTracker & NameNode host
* **path** : HDFS path
* **mode** : the mode of matrix multiplication

### Environments

* Ubuntu 14.04 (or other Linux distributions)
* JDK 7
* Hadoop 1.2.1 (RecommendationEngine utilizes **DistributedCache** to distribute the smaller files to nodes in cluster and caching them)
* Sqoop (I have used Sqoop in cron, you can ignore it.)

### Contact me

* Email : tinylcy@gmail.com
* My Website : http://tinylcy.me 

