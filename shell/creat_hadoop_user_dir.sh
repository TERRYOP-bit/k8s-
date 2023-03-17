#!/bin/bash

user=(11701080221)
for var in ${user[@]}
do
    hadoop fs -mkdir -p /spark_user/$var/code
    hadoop fs -mkdir -p /spark_user/$var/data
    hadoop fs -mkdir -p /spark_user/$var/result
    echo 'creat success' $var
done



