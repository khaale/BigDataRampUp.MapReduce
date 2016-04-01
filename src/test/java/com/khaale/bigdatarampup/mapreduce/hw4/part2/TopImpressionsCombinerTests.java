package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class TopImpressionsCombinerTests {

    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        TopImpressionsCombiner combiner = new TopImpressionsCombiner();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(combiner);
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new LongWritable(1), new LongWritable(2)));

        reduceDriver.withOutput(new Text("1"), new LongWritable(3));

        reduceDriver.runTest();
    }
}
