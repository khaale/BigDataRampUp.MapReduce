package com.khaale.bigdatarampup.mapreduce.hw4.part1.writables;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ImpressionKeyComparator extends WritableComparator {

    public ImpressionKeyComparator() {
        super(ImpressionKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        ImpressionKeyWritable key1 = (ImpressionKeyWritable) wc1;
        ImpressionKeyWritable key2 = (ImpressionKeyWritable) wc2;

        int cmp = key1.getiPinYouId().compareTo(key2.getiPinYouId());

        if (cmp == 0) {
            cmp = Long.compare(key1.getTimestamp(), key2.getTimestamp());
        }

        return cmp;
    }
}
