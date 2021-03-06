package org.dan.tracer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static org.dan.tracer.LogLineParser.readTimeStamp;
import static org.dan.tracer.LogLineParser.writeIntAsStr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class LogLineParserTest {
    public static ByteBuffer wrap(String s) {
        return ByteBuffer.wrap(s.getBytes()).order(LITTLE_ENDIAN);
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

    @Test
    public void formatZero() throws ParseException {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        parser.setTimeZone(TimeZone.getTimeZone("UTC"));
        parser.format(new Date(0));
        for (long date : asList(
                0L, 1000L, 60L * 1000L,
                3600L * 1000L, 24L * 3600L * 1000L,
                1000000L, 10000000L,
                parser.parse("1970-01-02 00:00:00.000").getTime(),
                parser.parse("1970-02-03 00:00:00.000").getTime(),
                parser.parse("1970-03-11 00:00:00.000").getTime(),
                parser.parse("1971-01-01 00:00:00.000").getTime(),
                parser.parse("1972-01-01 00:00:00.000").getTime(),
                parser.parse("1973-01-01 00:00:00.000").getTime(),
                parser.parse("1973-03-01 00:00:00.000").getTime(),
                parser.parse("1973-02-28 00:00:00.000").getTime(),
                parser.parse("1976-02-29 23:00:00.000").getTime(),
                parser.parse("1973-03-01 23:59:59.000").getTime(),
                parser.parse("2015-12-31 23:59:59.999").getTime(),
                System.currentTimeMillis())) {
            assertEquals(parser.format(new Date(date)), format(date));
        }
    }

    private String format(long ts) {
        ByteBuffer b = ByteBuffer.allocate(40).order(LITTLE_ENDIAN);
        LogLineParser.writeDateTime(b, ts);
        b.flip();
        return new String(b.array(), 0, b.limit());
    }

    @Test
    public void parseFirstTraceLine() {
        Dictionary dictionary = Dictionary.create();
        int[] callback = new int[1];
        RequestRepo repo = new RequestRepo(null, null, null) {
            @Override
            public void line(int serviceId, long requestId, long started, long ended, long callerSnapId, long snapId) {
                ++callback[0];
                assertEquals(1, serviceId);
                assertEquals(0L, started);
                assertEquals(asLong("4twlb5e6"), requestId);
                assertEquals(3L, ended);
                assertEquals(asLong("null->  ") & (~0L >>> 16), callerSnapId);
                assertEquals(asLong("vmrya5qg"), snapId);
            }
        };
        LogLineParser logLineParser = new LogLineParser(dictionary, repo);
        final long lastTs = logLineParser.parse(input(
                "1970-01-01T00:00:00.000Z 1970-01-01T00:00:00.003Z 4twlb5e6 service8 null->vmrya5qg\n"));

        assertEquals(1, callback[0]);
        assertEquals(3L, lastTs);
        assertEquals("service8", new String(dictionary.getById(1).array()));
    }

    private ByteBuffer input(String text) {
        return ByteBuffer.wrap(text.getBytes()).order(LITTLE_ENDIAN);
    }

    static long asLong(String s) {
        return ByteBuffer.wrap(s.getBytes()).order(LITTLE_ENDIAN).getLong();
    }

    @Test
    public void parseNextTraceLine() {
        Dictionary dictionary = Dictionary.create();
        int[] callback = new int[1];
        RequestRepo repo = new RequestRepo(null, null, null) {
            @Override
            public void line(int serviceId, long requestId, long started, long ended, long callerSnapId, long snapId) {
                ++callback[0];
                assertEquals(1, serviceId);
                assertEquals(0L, started);
                assertEquals(asLong("4twlb5e6"), requestId);
                assertEquals(3L, ended);
                assertEquals(asLong("tmrya5qt"), callerSnapId);
                assertEquals(asLong("vmrya5qg"), snapId);
            }
        };
        LogLineParser logLineParser = new LogLineParser(dictionary, repo);
        final long lastTs = logLineParser.parse(input(
                "1970-01-01T00:00:00.000Z 1970-01-01T00:00:00.003Z 4twlb5e6 ser8 tmrya5qt->vmrya5qg\n"));
        assertEquals(3L, lastTs);
        assertEquals(1, callback[0]);
        assertEquals("ser8", new String(dictionary.getById(1).array()));
    }

    @Test
    public void parseSearchNewLineNextTraceLine() {
        Dictionary dictionary = Dictionary.create();
        int[] callback = new int[1];
        RequestRepo repo = new RequestRepo(null, null, null) {
            @Override
            public void line(int serviceId, long requestId, long started, long ended, long callerSnapId, long snapId) {
                ++callback[0];
                assertEquals(1, serviceId);
                assertEquals(0L, started);
                assertEquals(asLong("4twlb5e6"), requestId);
                assertEquals(3L, ended);
                assertEquals(asLong("tmrya5qt"), callerSnapId);
                assertEquals(asLong("vmrya5qg"), snapId);
            }
        };
        LogLineParser logLineParser = new LogLineParser(dictionary, repo);
        logLineParser.setSearchNewLine(true);
        ByteBuffer input = input(
                "asdfasdfa 345234513 sdfa sdfasdf\n1970-01-01T00:00:00.000Z " +
                        "1970-01-01T00:00:00.003Z 4twlb5e6 ser8 tmrya5qt->vmrya5qg\n");
        assertEquals(0, logLineParser.parse(input));
        final long lastTs = logLineParser.parse(input);
        assertEquals(3L, lastTs);
        assertEquals(1, callback[0]);
        assertEquals("ser8", new String(dictionary.getById(1).array()));
    }

    @Test
    public void turnOnSearchNewLine() {
        Dictionary dictionary = Dictionary.create();
        int[] callback = new int[1];
        RequestRepo repo = new RequestRepo(null, null, null) {
            @Override
            public void line(int serviceId, long requestId, long started, long ended, long callerSnapId, long snapId) {
                ++callback[0];
            }
        };
        LogLineParser logLineParser = new LogLineParser(dictionary, repo);
        logLineParser.setSearchNewLine(true);
        ByteBuffer input = input("1970-01-01T00:00:00.000Z~");
        assertEquals(0, logLineParser.parse(input));
        assertEquals(0, callback[0]);
        assertTrue(logLineParser.isSearchNewLine());
    }

    @Test
    public void write2DigitIntAsStr() {
        assertEquals("00", writeIntAsStr(0, 2));
        assertEquals("01", writeIntAsStr(1, 2));
        assertEquals("09", writeIntAsStr(9, 2));
        assertEquals("10", writeIntAsStr(10, 2));
        assertEquals("99", writeIntAsStr(99, 2));
        assertEquals("000", writeIntAsStr(0, 3));
        assertEquals("001", writeIntAsStr(1, 3));
        assertEquals("999", writeIntAsStr(999, 3));
        assertEquals("1999", writeIntAsStr(1999, 4));
        assertEquals("2017", writeIntAsStr(2017, 4));
    }

    private String writeIntAsStr(int n, int width) {
        ByteBuffer buf = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
        LogLineParser.writeIntAsStr(buf, n, width);
        return new String(buf.array(), 0, width);
    }
}
