package com.khaale.bigdatarampup.mapreduce.hw3.part2.parsing;

import org.apache.hadoop.io.Text;

/**
 * Created by Aleksander_Khanteev on 3/30/2016.
 */
public class IpDataParser {

    private static final int delimeter = '\t';
    private String ip;
    private String userAgent;
    private Double biddingPrice;

    public void set(Text input) {

        String line = input.toString();

        int beginIndex;
        int endIndex = 0;

        for(int i = 0; i < 19; i++) {

            beginIndex = endIndex + 1;
            endIndex = line.indexOf(delimeter, beginIndex);

            if (i == 3) {
                userAgent = line.substring(beginIndex, endIndex);
            } else if (i == 4) {
                ip = line.substring(beginIndex, endIndex);
            } if (i == 18) {
                biddingPrice = Double.valueOf(line.substring(beginIndex, endIndex));
            }
        }
    }

    public String getIp() {
        return ip;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public Double getBiddingPrice() {
        return biddingPrice;
    }
}
