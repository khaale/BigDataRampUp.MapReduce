package com.khaale.bigdatarampup.mapreduce.hw3.part2;

import com.khaale.bigdatarampup.mapreduce.hw3.part2.parsing.BrowserDetector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BrowserDetectorTest {

    private final String userAgent;
    private final Browsers expectedBrowser;

    @Parameters(name = "{index}: UA \"{0}\" -> Browser {1}")
    public static Collection userAgents() {
        return Arrays.asList(new Object[][] {
                //Chrome
                { "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36", Browsers.CHROME },
                //FireFox
                { "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1", Browsers.FIREFOX},
                //IE
                { "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko", Browsers.IE },
                { "Mozilla/5.0 (Windows; U; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)", Browsers.IE },
                //Opera
                { "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16", Browsers.OPERA },
                //Other
                { "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A", Browsers.OTHER }
        });
    }

    public BrowserDetectorTest(String userAgent, Browsers expectedBrowser) {

        this.userAgent = userAgent;
        this.expectedBrowser = expectedBrowser;
    }

    @Test
    public void testGetBrowserByUserAgent() {
        //act


        BrowserDetector sut = new BrowserDetector();
        Browsers actualBrowser = sut.getBrowserByUserAgent(userAgent);
        //assert
        assertEquals(expectedBrowser, actualBrowser);
    }

}
