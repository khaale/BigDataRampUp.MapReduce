package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopImpressionsCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable outValue = new LongWritable();


    @Override
    public void reduce(Text inputKey, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

        long count = 0;
        for (LongWritable value : values){
            count += value.get();
        }

        outValue.set(count);
        output.write(inputKey, outValue);
    }
}
