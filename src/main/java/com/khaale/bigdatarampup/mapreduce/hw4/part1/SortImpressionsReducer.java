package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortImpressionsReducer extends Reducer<ImpressionKeyWritable, NullWritable, Text, LongWritable> {

    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable();


    @Override
    public void reduce(ImpressionKeyWritable inputKey, Iterable<NullWritable> values, Context output) throws IOException, InterruptedException {

        for(NullWritable value : values) {
            outKey.set(inputKey.getiPinYouId());
            outValue.set(inputKey.getTimestamp());
            output.write(outKey, outValue);
        }
    }
}
