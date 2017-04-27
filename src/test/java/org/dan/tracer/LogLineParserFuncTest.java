package org.dan.tracer;

import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.asList;
import static org.dan.tracer.LogLineParser.readEpochTimeStamp;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class LogLineParserFuncTest {
    private DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

    @Test
    public void parseDate() {
        for (String date : asList("2013-10-23T10:15:34.906Z", "2017-10-23T10:15:34.906Z",
                "2016-10-23T10:15:34.906Z", "2016-02-23T10:15:34.906Z",
                "2016-02-28T10:15:34.906Z", "2016-01-28T10:15:34.906Z")) {
            TemporalAccessor expectedParsed = formatter.parse(date);
            long expected = expectedParsed.getLong(ChronoField.NANO_OF_SECOND) +
                    expectedParsed.getLong(ChronoField.INSTANT_SECONDS);
            assertEquals(date, expected, readEpochTimeStamp(wrap((date + " ").getBytes())));
        }
    }
}
