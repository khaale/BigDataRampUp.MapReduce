#!/bin/bash  

# build fat jar
mvn package || { echo 'mvn failed' ; exit 1; }

DICT_DIR=hdfs:///data/advertising/
INPUT_DIR=hdfs:///data/advertising/raw/*.*
OUTPUT_DIR=hdfs:///data/advertising/out

# HW 3, Part 1: 
hdfs dfs -copyFromLocal -f data/user.profile.tags.us.txt $DICT_DIR
hdfs dfs -rm -r -f -skipTrash $OUTPUT_DIR/user-tags
yarn jar ./target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar com.khaale.bigdatarampup.mapreduce.hw3.part1.UserTagsDriver $INPUT_DIR $OUTPUT_DIR/user-tags $DICT_DIR

# HW 3, Part 2: 
hdfs dfs -rm -r -f -skipTrash $OUTPUT_DIR/ip-stats
yarn jar ./target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar com.khaale.bigdatarampup.mapreduce.hw3.part2.IpStatsDriver $INPUT_DIR $OUTPUT_DIR/ip-stats

# HW 4, Part 1: 
hdfs dfs -rm -r -f -skipTrash $OUTPUT_DIR/sort-impressions
yarn jar ./target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar com.khaale.bigdatarampup.mapreduce.hw4.part1.SortImpressionsDriver $INPUT_DIR $OUTPUT_DIR/sort-impressions 1

# HW 4, Part 2: 
yarn jar ./target/mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar com.khaale.bigdatarampup.mapreduce.hw4.part2.TopImpressionsDriver $INPUT_DIR 10
