package com.khaale.bigdatarampup.mapreduce.hw3.part2.parsing;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.Browsers;
import org.apache.commons.lang.StringUtils;

/**
 * Created by Aleksander_Khanteev on 3/31/2016.
 */
public class BrowserDetector {

    public Browsers getBrowserByUserAgent(String userAgent) {

        Browsers result;

        if (StringUtils.containsIgnoreCase(userAgent, "MSIE") || StringUtils.containsIgnoreCase(userAgent, "Trident")) {
            result = Browsers.IE;
        } else if (StringUtils.containsIgnoreCase(userAgent, "opera")) {
            result = Browsers.OPERA;
        } else if (StringUtils.containsIgnoreCase(userAgent, "firefox")) {
            result = Browsers.FIREFOX;
        } else if (StringUtils.containsIgnoreCase(userAgent, "(KHTML, like Gecko) Chrome")) {
            result = Browsers.CHROME;
        } else {
            result = Browsers.OTHER;
        }

        return result;
    }
}
