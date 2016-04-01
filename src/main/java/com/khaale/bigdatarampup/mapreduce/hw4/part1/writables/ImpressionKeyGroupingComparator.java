package com.khaale.bigdatarampup.mapreduce.hw4.part1.writables;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ImpressionKeyGroupingComparator extends WritableComparator {

    public ImpressionKeyGroupingComparator() {
        super(ImpressionKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        ImpressionKeyWritable key1 = (ImpressionKeyWritable) wc1;
        ImpressionKeyWritable key2 = (ImpressionKeyWritable) wc2;

        return key1.getiPinYouId().compareTo(key2.getiPinYouId());
    }
}

