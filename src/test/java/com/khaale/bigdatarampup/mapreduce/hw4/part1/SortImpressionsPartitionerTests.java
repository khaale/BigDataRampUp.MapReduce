package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Due to impossibility to mock String.hashCode , I have to use hardcoded value there.
 * It involves an assumption that String.hashCode implementation is stable.
 * I think that rely on hasCode it's a bad idea in general, but in this specific case it seems to be a lesser evil.
 * See discussion http://stackoverflow.com/questions/785091/consistency-of-hashcode-on-a-java-string *
 */
public class SortImpressionsPartitionerTests {

    @Test
    public void testGetPartition_shouldWorkWithIpinyouIdWithNegativeHashCode() {
        //arrange
        ImpressionKeyWritable keyWritable = new ImpressionKeyWritable("random", 1);

        //act
        SortImpressionsPartitioner sut = new SortImpressionsPartitioner();
        int actual = sut.getPartition(keyWritable, new LongWritable(1), 10);

        //assert
        assertTrue("Partition count " + actual + " should be greater or equal to 0", actual >= 0);
    }
}
