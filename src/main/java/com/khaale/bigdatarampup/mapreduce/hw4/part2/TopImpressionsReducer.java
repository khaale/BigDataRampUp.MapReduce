package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopImpressionsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private String maxKey;
    private long maxCount;

    @Override
    protected void setup(Context output) {
        maxKey = null;
        maxCount = 0;
    }

    @Override
    public void reduce(Text inputKey, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

        long count = 0;
        for(LongWritable value : values) {
            count += value.get();
        }

        if (count  > maxCount) {
            maxCount = count;
            maxKey = inputKey.toString();
        }
    }

    @Override
    protected void cleanup(Context output) {

        output.getCounter(CounterGroups.TopImpressions, maxKey).setValue(maxCount);
    }


}
