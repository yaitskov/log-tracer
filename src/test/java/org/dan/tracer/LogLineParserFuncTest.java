package org.dan.tracer;

import static java.util.Arrays.asList;
import static org.dan.tracer.LogLineParser.readTimeStamp;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class LogLineParserFuncTest {
    private DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

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
    public void parseDate() {
        for (String date : asList("2013-10-23T10:15:34.906Z", "2017-10-23T10:15:34.906Z",
                "2016-10-23T10:15:34.906Z", "2016-02-23T10:15:34.906Z",
                "2016-02-28T10:15:34.906Z", "2016-01-28T10:15:34.906Z")) {
            TemporalAccessor expectedParsed = formatter.parse(date);
            long expected = expectedParsed.getLong(ChronoField.NANO_OF_SECOND) +
                    expectedParsed.getLong(ChronoField.INSTANT_SECONDS);
            assertEquals(date, expected, readTimeStamp(wrap(date + " ")));
        }
    }
}
