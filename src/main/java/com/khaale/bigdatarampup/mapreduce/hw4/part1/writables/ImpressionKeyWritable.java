package com.khaale.bigdatarampup.mapreduce.hw4.part1.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */


public class ImpressionKeyWritable implements WritableComparable<ImpressionKeyWritable> {

    private String iPinYouId;
    private long timestamp;

    public ImpressionKeyWritable() {
        set("", 0L);
    }

    public ImpressionKeyWritable(String iPinYouId, long timestamp) {
        set(iPinYouId, timestamp);
    }

    public void set(String iPinYouId, Long timestamp) {
        this.iPinYouId = iPinYouId;
        this.timestamp = timestamp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(iPinYouId);
        dataOutput.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        iPinYouId = dataInput.readUTF();
        timestamp = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "IpStatsWritable{" +
                "iPinYouId=" + iPinYouId +
                ", timestamp=" + timestamp +
                '}';
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getiPinYouId() {
        return iPinYouId;
    }

    @Override
    public int compareTo(ImpressionKeyWritable that) {

        if (this == that) return 0;

        int comp = iPinYouId.compareTo(that.iPinYouId);

        if (comp == 0) {
            comp = Long.compare(timestamp, that.timestamp);
        }

        return comp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImpressionKeyWritable that = (ImpressionKeyWritable) o;

        if (timestamp != that.timestamp) return false;
        return iPinYouId != null ? iPinYouId.equals(that.iPinYouId) : that.iPinYouId == null;

    }

    @Override
    public int hashCode() {
        int result = iPinYouId != null ? iPinYouId.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
