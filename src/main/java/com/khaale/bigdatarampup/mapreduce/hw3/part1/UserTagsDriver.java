package com.khaale.bigdatarampup.mapreduce.hw3.part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by Aleksander_Khanteev on 3/28/2016.
 */
public class UserTagsDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: [hadoop-input] [hadoop-output] [hadoop-dict-data-dir]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(UserTagsMapper.class);
        job.setCombinerClass(UserTagsReducer.class);
        job.setReducerClass(UserTagsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(UserTagsDriver.class);

        job.addCacheFile(new URI(args[2] + "user.profile.tags.us.txt"));

        job.submit();

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int retCode = ToolRunner.run(new UserTagsDriver(), args);
        System.exit(retCode);
    }
}
