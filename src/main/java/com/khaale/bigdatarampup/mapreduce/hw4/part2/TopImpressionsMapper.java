package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import com.khaale.bigdatarampup.mapreduce.hw4.part2.parsing.ImpressionsParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopImpressionsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final ImpressionsParser parser = new ImpressionsParser();
    private final Text outKey = new Text();
    private static final LongWritable outValue = new LongWritable(1);

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context output) throws IOException, InterruptedException {

        parser.set(inputValue);

        if (!"null".equals(parser.getiPinyouId()) && parser.getStreamId() == 1) {
            outKey.set(parser.getiPinyouId());
            output.write(outKey, outValue);
        }
        else {
            output.getCounter(Counters.SKIPPED_LINE).increment(1);
        }
    }
}
