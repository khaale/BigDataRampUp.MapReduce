package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Aleksander_Khanteev on 3/29/2016.
 */
public class SortImpressionsMapperTests {

    MapDriver<LongWritable, Text, ImpressionKeyWritable, NullWritable> mapDriver;

    @Before
    public void setUp() {

        SortImpressionsMapper mapper = new SortImpressionsMapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(
                "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t1"
        ));

        mapDriver.withOutput(new ImpressionKeyWritable("Vh16OwT6OQNUXbj",20130606000104000L), NullWritable.get());

        mapDriver.runTest();
    }

    @Test
    public void testMapper_shouldSkip_withStreamIdNot1() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(
                "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0"
        ));

        mapDriver.withAllOutput(new ArrayList<>());

        mapDriver.runTest();
    }

    @Test
    public void testMapper_shouldSkip_withEmptyIPinyouId() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(
                "b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tnull\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t1"
        ));

        mapDriver.withAllOutput(new ArrayList<>());

        mapDriver.runTest();
    }
}
