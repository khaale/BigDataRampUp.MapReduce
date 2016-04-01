package com.khaale.bigdatarampup.mapreduce.hw3.part2.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */


public class IpStatsWritable implements WritableComparable<IpStatsWritable> {

    private long visits;
    private double spends;

    public IpStatsWritable() {
        set(0L, 0d);
    }

    public IpStatsWritable(long count, double spends) {
        set(count, spends);
    }

    public void set(Long visits, Double bidding) {
        this.visits = visits;
        this.spends = bidding;
    }

    @Override
    public int compareTo(IpStatsWritable o) {
        int cmp = Long.compare(visits, o.visits);
        if (cmp != 0) {
            return cmp;
        }
        return Double.compare(spends, o.spends);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IpStatsWritable) {
            IpStatsWritable other = (IpStatsWritable) o;
            return visits == other.visits && spends == other.spends;
        }
        return false;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(visits);
        dataOutput.writeDouble(spends);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        visits = dataInput.readLong();
        spends = dataInput.readDouble();
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) (visits ^ (visits >>> 32));
        temp = Double.doubleToLongBits(spends);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "IpStatsWritable{" +
                "visits=" + visits +
                ", spends=" + spends +
                '}';
    }

    public double getSpends() {
        return spends;
    }

    public long getVisits() {
        return visits;
    }
}
