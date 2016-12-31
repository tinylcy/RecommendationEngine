#!/bin/bash

source config.cfg
filename=$filename     # input file
reducer=$reducer       # the number of reducers
n=$n                   # the number of items
m=$m                   # the number of users
p=$p                   # matrix block size
q=$q                   # matrix block size
r=$r                   # matrix block size
host=$host             # JobTracker & NameNode host
path=$path             # HDFS path
mode=$mode             # the mode of matrix multiplication

command="hadoop jar RecommendationEngine.jar -filename $filename -reducer $reducer -n $n -m $m -p $p -q $q -r $r -host $host -path $path -mode $mode"

echo "command = $command"

$command
echo ""
