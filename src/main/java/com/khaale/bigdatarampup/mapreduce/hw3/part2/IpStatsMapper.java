package com.khaale.bigdatarampup.mapreduce.hw3.part2;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.parsing.BrowserDetector;
import com.khaale.bigdatarampup.mapreduce.hw3.part2.parsing.IpDataParser;
import com.khaale.bigdatarampup.mapreduce.hw3.part2.writables.IpStatsWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpStatsMapper extends Mapper<LongWritable, Text, Text, IpStatsWritable> {

    private IpDataParser parser = new IpDataParser();
    private BrowserDetector browserDetector = new BrowserDetector();

    private Text outKey = new Text();
    private IpStatsWritable outValue = new IpStatsWritable();

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context output) throws IOException, InterruptedException {

        parser.set(inputValue);

        outKey.set(parser.getIp());
        outValue.set(1L, parser.getBiddingPrice());

        populateUserAgentCounters(parser.getUserAgent(), output);

        output.write(outKey, outValue);

    }

    private void populateUserAgentCounters(String userAgent, Context output) {

        Browsers browser = browserDetector.getBrowserByUserAgent(userAgent);
        output.getCounter(browser).increment(1);
    }
}
