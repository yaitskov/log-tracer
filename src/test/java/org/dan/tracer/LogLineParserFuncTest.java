package org.dan.tracer;

import static java.util.Arrays.asList;
import static org.dan.tracer.LogLineParser.readTimeStamp;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class LogLineParserFuncTest {
    public static ByteBuffer wrap(String s) {
        return ByteBuffer.wrap(s.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void parseZeroDate() {
       assertEquals(0L, readTimeStamp(wrap("1970-01-01T00:00:00.000Z ")));
    }

    @Test
    public void parseFirstSecond() {
        assertEquals(1000L, readTimeStamp(wrap("1970-01-01T00:00:01.000Z ")));
    }

    @Test
    public void parseHourSecond() {
        assertEquals(3600 * 1000L, readTimeStamp(wrap("1970-01-01T01:00:00.000Z ")));
    }

    @Test
    public void parseMinuteSecond() {
        assertEquals(60 * 1000L, readTimeStamp(wrap("1970-01-01T00:01:00.000Z ")));
    }

    @Test
    public void parseDate() throws ParseException {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        for (String date : asList(
                "1970-01-01T00:00:00.000Z",
                "1971-01-01T00:00:00.000Z",
                "1972-01-01T00:00:00.000Z",
                "1973-01-01T00:00:00.000Z",
                "2013-01-01T00:00:00.000Z",
                "2013-10-23T10:15:34.906Z", "2017-10-23T10:15:34.906Z",
                "2016-10-23T10:15:34.906Z", "2016-02-23T10:15:34.906Z",
                "2016-02-28T10:15:34.906Z", "2016-01-28T10:15:34.906Z")) {
            Date d = parser.parse(date);
            assertEquals(date, d.getTime(), readTimeStamp(wrap(date + " ")));
        }
    }
}
