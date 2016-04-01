package com.khaale.bigdatarampup.mapreduce.hw4.part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */
public class TopImpressionsDriver extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(TopImpressionsDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: [input] [num-reducers]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TopImpressionsMapper.class);
        job.setCombinerClass(TopImpressionsCombiner.class);
        job.setReducerClass(TopImpressionsReducer.class);
        job.setNumReduceTasks(Integer.valueOf(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setJarByClass(TopImpressionsDriver.class);

        job.submit();

        job.waitForCompletion(true);

        printTopImpressions(job.getCounters().getGroup(CounterGroups.TopImpressions));

        return 0;
    }

    private void printTopImpressions(CounterGroup group) {

        Optional<Counter> maxCounter = StreamSupport.stream(group.spliterator(), false)
            .max(Comparator.comparing(Counter::getValue));

        if (maxCounter.isPresent()) {
            logger.info("TOP IMPRESSIONS: iPinyouId = {}, imp. count = {}", maxCounter.get().getName(), maxCounter.get().getValue());
        }
        else {
            logger.warn("Can't get max impressions - there are no counters!");
        }
    }

    public static void main(String[] args) throws Exception {
        int retCode = ToolRunner.run(new TopImpressionsDriver(), args);
        System.exit(retCode);
    }
}
