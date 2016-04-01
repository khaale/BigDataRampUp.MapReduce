package com.khaale.bigdatarampup.mapreduce.hw4.part1;

import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyComparator;
import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyGroupingComparator;
import com.khaale.bigdatarampup.mapreduce.hw4.part1.writables.ImpressionKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */
public class SortImpressionsDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: [input] [output] [numReducers]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImpressionKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(SortImpressionsMapper.class);
        job.setReducerClass(SortImpressionsReducer.class);
        job.setNumReduceTasks(Integer.valueOf(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(SortImpressionsPartitioner.class);
        job.setSortComparatorClass(ImpressionKeyComparator.class);
        job.setGroupingComparatorClass(ImpressionKeyGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(SortImpressionsDriver.class);

        job.submit();

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int retCode = ToolRunner.run(new SortImpressionsDriver(), args);
        System.exit(retCode);
    }
}
