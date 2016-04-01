package com.khaale.bigdatarampup.mapreduce.hw3.part2;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.writables.IpStatsWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class IpStatsReducerTests {

    ReduceDriver<Text, IpStatsWritable, Text, IpStatsWritable> reduceDriver;

    @Before
    public void setUp() {
        IpStatsReducer reducer = new IpStatsReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("127.0.0.*"), Arrays.asList(new IpStatsWritable(1, 10), new IpStatsWritable(2, 20)));

        reduceDriver.withOutput(new Text("127.0.0.*"), new IpStatsWritable(3, 30));

        reduceDriver.runTest();
    }
}
