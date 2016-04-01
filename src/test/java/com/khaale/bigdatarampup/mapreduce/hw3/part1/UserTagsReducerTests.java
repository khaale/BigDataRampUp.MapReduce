package com.khaale.bigdatarampup.mapreduce.hw3.part1;

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
public class UserTagsReducerTests {

    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        UserTagsReducer reducer = new UserTagsReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("tag"), Arrays.asList(new LongWritable(1), new LongWritable(2)));

        reduceDriver.withOutput(new Text("tag"), new LongWritable(3));

        reduceDriver.runTest();
    }
}
