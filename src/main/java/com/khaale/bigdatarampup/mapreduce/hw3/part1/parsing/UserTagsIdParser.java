package com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing;

import org.apache.hadoop.io.Text;

public interface UserTagsIdParser {
    void set(Text row);
    long getUserTagsId();
}


