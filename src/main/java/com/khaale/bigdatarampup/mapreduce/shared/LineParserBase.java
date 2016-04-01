package com.khaale.bigdatarampup.mapreduce.shared;

import org.apache.hadoop.io.Text;

/**
 * Base class for text line parsers.
 */
public abstract class LineParserBase {

    private static final int delimiter = '\t';

    public void set(Text input) {

        String line = input.toString();

        int beginIndex;
        int endIndex = 0;

        for(int i = 0; i < 22; i++) {

            beginIndex = endIndex + 1;
            endIndex = line.indexOf(delimiter, beginIndex);

            if (endIndex == -1) {
                endIndex = line.length();
            }

            onField(line, i, beginIndex, endIndex);
        }
    }

    /**
     * Processes field
     * @param line - source text line
     * @param fieldIndex - current field index
     * @param fieldBegin - current field begin index
     * @param fieldEnd - current field end index
     * @return true when we don't need to process next fields.
     */
    protected abstract boolean onField(String line, int fieldIndex, int fieldBegin, int fieldEnd);
}
