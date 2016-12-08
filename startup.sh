#!/bin/bash

source config.cfg
filename=$filename
reducer=$reducer
n=$n
m=$m
p=$p
q=$q
r=$r
host=$host
path=$path
mode=$mode

command="hadoop jar RecommendationEngine.jar -filename $filename -reducer $reducer -n $n -m $m -p $p -q $q -r $r -host $host -path $path -mode $mode"

echo "command = $command"

$command
echo ""
