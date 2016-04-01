package com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing;

import org.apache.hadoop.io.Text;

public class IndexOfUserTagsIdParser implements UserTagsIdParser {

    private String line;
    private static final int delimeter = '\t';


    public void set(Text row) {
        line = row.toString();
    }

    public long getUserTagsId() {

        int beginIndex = 0;
        int endIndex = 0;

        for(int i = 0; i < 21; i++) {
            beginIndex = endIndex;
            endIndex = line.indexOf(delimeter, beginIndex+1);
        }

        return Long.valueOf(line.substring(beginIndex+1, endIndex));
    }
}
