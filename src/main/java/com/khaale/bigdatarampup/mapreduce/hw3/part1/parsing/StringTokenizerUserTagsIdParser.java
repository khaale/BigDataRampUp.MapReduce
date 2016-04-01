package com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing;

import org.apache.hadoop.io.Text;

import java.util.StringTokenizer;

public class StringTokenizerUserTagsIdParser implements UserTagsIdParser {

    private String line;
    private static final int delimeter = '\t';

    @Override
    public void set(Text row) {
        line = row.toString();
    }

    @Override
    public long getUserTagsId() {

        StringTokenizer st = new StringTokenizer(line, "\t");

        for(int i = 0; i < 19; i++) {
            st.nextToken();
        }

        String value = st.nextToken();
        return  Long.valueOf(value);
    }
}
