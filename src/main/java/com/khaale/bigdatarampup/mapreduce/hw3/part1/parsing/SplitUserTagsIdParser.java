package com.khaale.bigdatarampup.mapreduce.hw3.part1.parsing;

import org.apache.hadoop.io.Text;

public class SplitUserTagsIdParser implements UserTagsIdParser {

    private String[] cells;


    public void set(Text row) {
        cells = row.toString().split("\\t");
    }

    public long getUserTagsId() {

        return Long.valueOf(cells[20]);
    }
}
