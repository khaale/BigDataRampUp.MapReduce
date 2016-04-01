package com.khaale.bigdatarampup.mapreduce.hw3.part1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class UserTagsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable result = new LongWritable(1);

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (LongWritable val : values)
        {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
