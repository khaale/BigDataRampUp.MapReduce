package com.khaale.bigdatarampup.mapreduce.hw3.part2;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.writables.IpStatsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IpStatsReducer extends Reducer<Text, IpStatsWritable, Text, IpStatsWritable> {


    private IpStatsWritable result = new IpStatsWritable();

    @Override
    public void reduce(Text inputKey, Iterable<IpStatsWritable> values, Context output) throws IOException, InterruptedException {

        long count = 0;
        double bidding = 0;
        for(IpStatsWritable value : values) {
            count += value.getVisits();
            bidding += value.getSpends();
        }

        result.set(count, bidding);
        output.write(inputKey, result);
    }
}
