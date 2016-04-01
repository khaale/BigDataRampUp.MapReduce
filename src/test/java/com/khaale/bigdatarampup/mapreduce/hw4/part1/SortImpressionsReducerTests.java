package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.KeyValueReuseList;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class SortImpressionsReducerTests {

    ReduceDriver<ImpressionKeyWritable, NullWritable, Text, LongWritable> driver;

    @Before
    public void setUp() {

        SortImpressionsReducer reducer = new SortImpressionsReducer();
        driver = new ReduceDriver<>();
        driver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {

        KeyValueReuseList<ImpressionKeyWritable, NullWritable> input = new KeyValueReuseList<>(new ImpressionKeyWritable(), NullWritable.get(), driver.getConfiguration());
        input.add(new Pair<>(new ImpressionKeyWritable("1", 1), NullWritable.get()));
        input.add(new Pair<>(new ImpressionKeyWritable("1", 2), NullWritable.get()));
        input.add(new Pair<>(new ImpressionKeyWritable("2", 1), NullWritable.get()));
        driver.withInput(input);

        driver.withOutput(new Text("1"), new LongWritable(1));
        driver.withOutput(new Text("1"), new LongWritable(2));
        driver.withOutput(new Text("2"), new LongWritable(1));

        driver.runTest();
    }
}
