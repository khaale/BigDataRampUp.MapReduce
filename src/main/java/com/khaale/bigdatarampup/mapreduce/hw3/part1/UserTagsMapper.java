package com.khaale.bigdatarampup.mapreduce.hw3.part1;

import com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing.UserTagsIdParser;
import com.khaale.bigdatarampup.mapreduce.shared.ConfigurableFilePathProvider;
import com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing.IndexOfUserTagsIdParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Aleksander_Khanteev on 3/28/2016.
 */
public class UserTagsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static Logger logger = LoggerFactory.getLogger(UserTagsMapper.class);

    private UserTagsIdParser mapper;
    private Map<Long, String[]> userTagsDictionary;

    private final LongWritable one = new LongWritable(1L);
    private final Text word = new Text();


    protected void setup(Context context) throws IOException, InterruptedException {

        mapper = new IndexOfUserTagsIdParser();

        loadUserTagsDictionary(context);

        super.setup(context);
    }

    private void loadUserTagsDictionary(Context context) throws IOException {

        Path userTagsDictionaryPath = new ConfigurableFilePathProvider().getUserTagDictionaryPath(context);

        userTagsDictionary = Files.lines(userTagsDictionaryPath)
                .skip(1)
                .map(s -> s.split("\\t"))
                .collect(Collectors.toMap(
                        r -> Long.valueOf(r[0]),
                        r -> r[1].split(",")));
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context output) throws IOException, InterruptedException {

        mapper.set(inputValue);

        long userTagsId = mapper.getUserTagsId();
        String[] tags = userTagsDictionary.get(userTagsId);

        if (tags != null) {
            for (String tag : tags) {
                word.set(tag);
                output.write(word, one);
            }
        }
        else if (userTagsId == 0) {
            output.getCounter(Counters.MISSED_TAGS_ZERO).increment(1);
        }
        else {
            output.getCounter(Counters.MISSED_TAGS).increment(1);
            logger.warn("Missed tags for: {}. Line: {}", userTagsId, inputValue.toString());
        }
    }
}

