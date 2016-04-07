package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */
public class SortImpressionsPartitioner extends Partitioner<ImpressionKeyWritable, LongWritable> {

    @Override
    public int getPartition(ImpressionKeyWritable impressionKeyWritable, LongWritable longWritable, int numPartitions) {
        return (impressionKeyWritable.getiPinYouId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

