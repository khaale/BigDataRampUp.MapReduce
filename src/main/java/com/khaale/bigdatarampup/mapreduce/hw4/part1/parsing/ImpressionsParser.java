package com.khaale.bigdatarampup.mapreduce.hw4.part1.parsing;

import com.khaale.bigdatarampup.mapreduce.shared.Columns;
import com.khaale.bigdatarampup.mapreduce.shared.LineParserBase;

/**
 * Extracts iPinyouId, Timestamp and StreamId from input line.
 */
public class ImpressionsParser extends LineParserBase {

    private String iPinyouId;
    private long timestamp;
    private int streamId;

    @Override
    protected boolean onField(String line, int fieldIndex, int fieldBegin, int fieldEnd) {
        boolean isLastField = false;

        if (fieldIndex == Columns.Timestamp) {
            timestamp = Long.valueOf(line.substring(fieldBegin, fieldEnd));
        } else if (fieldIndex == Columns.IPinyouId) {
            iPinyouId = line.substring(fieldBegin, fieldEnd);
        } if (fieldIndex == Columns.StreamId) {
            streamId = Integer.valueOf(line.substring(fieldBegin, fieldEnd));
            isLastField = true;
        }

        return isLastField;
    }

    public String getiPinyouId() {
        return iPinyouId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getStreamId() {
        return streamId;
    }
}