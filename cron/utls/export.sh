#!/bin/bash

mysql -u root -p112358 <<EOF
use Recommend;
truncate Recommend.RecommendInfo;
