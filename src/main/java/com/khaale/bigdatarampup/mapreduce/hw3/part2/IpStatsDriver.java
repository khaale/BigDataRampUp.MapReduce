package com.khaale.bigdatarampup.mapreduce.hw3.part2;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.writables.IpStatsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Aleksander_Khanteev on 3/28/2016.
 */
public class IpStatsDriver extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(IpStatsDriver.class);


    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        //set up compression
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpStatsWritable.class);

        job.setMapperClass(IpStatsMapper.class);
        job.setCombinerClass(IpStatsReducer.class);
        job.setReducerClass(IpStatsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(IpStatsDriver.class);

        job.submit();

        job.waitForCompletion(true);

        printBrowserStats(job.getCounters().getGroup(Browsers.class.getName()));

        return 0;
    }

    private void printBrowserStats(CounterGroup group) {
        group.forEach(c -> logger.info("{}: {}", c.getDisplayName(), c.getValue()));
    }

    public static void main(String[] args) throws Exception {
        int retCode = ToolRunner.run(new IpStatsDriver(), args);
        System.exit(retCode);
    }
}
