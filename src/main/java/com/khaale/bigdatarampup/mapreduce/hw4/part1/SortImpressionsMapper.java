package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.parsing.ImpressionsParser;
import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortImpressionsMapper extends Mapper<LongWritable, Text, ImpressionKeyWritable, NullWritable> {


    private ImpressionsParser parser = new ImpressionsParser();

    private ImpressionKeyWritable outKey = new ImpressionKeyWritable();
    private static final NullWritable outValue = NullWritable.get();

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context output) throws IOException, InterruptedException {

        parser.set(inputValue);

        if (!"null".equals(parser.getiPinyouId()) && parser.getStreamId() == 1) {
            outKey.set(parser.getiPinyouId(), parser.getTimestamp());
            output.write(outKey, outValue);
        }
        else {
            output.getCounter(Counters.SKIPPED_LINE).increment(1);
        }
    }
}
