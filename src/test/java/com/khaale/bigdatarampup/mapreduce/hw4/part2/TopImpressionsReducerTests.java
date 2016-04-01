package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class TopImpressionsReducerTests {

    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        TopImpressionsReducer reducer = new TopImpressionsReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer_shouldNotProduceOutput() throws IOException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new LongWritable(1), new LongWritable(2)));

        reduceDriver.withAllOutput(new ArrayList<>());

        reduceDriver.runTest();
    }

    @Test
    public void testReducer_shouldReturnMaxCount_inCounter() throws IOException {
        reduceDriver.withInput(new Text("1"), Arrays.asList(new LongWritable(1), new LongWritable(2)));
        reduceDriver.withInput(new Text("2"), Arrays.asList(new LongWritable(10), new LongWritable(20)));
        reduceDriver.withInput(new Text("3"), Arrays.asList(new LongWritable(1), new LongWritable(2)));

        reduceDriver.withCounter(CounterGroups.TopImpressions, "2", 30);

        reduceDriver.runTest();
    }
}
