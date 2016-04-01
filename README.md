# BigDataRampUp.MapReduce
Big Data Rump Up training, 3rd and 4rd homeworks: MapReduce on stream data.

## How to run

```bash
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


```

## How does it work
### HW 1
#### Part 1
Calculating user tags frequency.

1. Map
  - User tag from distributed cache are joined with stream
  - Producing records: Key = _tag_, Value = initial _count_ (1).
2. Combine / Reduce
  - _count_'s are summed.

Output is written on the disk in a text format.

#### Part 2
Calculating ip statistics (a count of visits and sum of bids)

1. Map
  - Producing records: Key = _ip_, Value = initial _count_ (1) and _bidding_
  - Incrementing browser's counters 
2. Combine / Reduce
  - _count_'s and _bidding_'s are summed

Output is written as snappy-compressed sequence file.
Driver prints browser statistics to stdout.

### HW 4
#### Part 1
Sorting site impressions by iPinyouId and timestamp.

1. Map
  - Filtering records by _streamId_ = 1
  - Producing records: Key = _iPinyouId_, _timestamp_, without value
2. Partition
  - partition records by _iPinyouId_ only
3. Group sort
  - Sorting records by _iPinyouId_ only
4. Secondary sort
  - Sorting records by _iPinyouId_ and _timestamp_
5. Reduce
  - Transforming records to Key = _iPinyouId_, Value = _timestamp_
  - Writing to output without aggregation

Output is written as text file.

#### Part 2
Printing iPinyouId with max impressions

1. Map
  - Producing records: Key = _iPinyouId_, Value = initial _count_ (1)
2. Combine
  - _count_'s are summed.
3. Reduce
  - _count_'s are summed, but not written to output. Only max sum of _count_'s with corresponding value are stored in a reducer's internal field.
  - On reducer's cleanup this stored maximal pair is sent to driver via counter.

Driver prints a counter with maximal value to stdout.
  
  


