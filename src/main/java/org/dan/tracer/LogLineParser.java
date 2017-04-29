package org.dan.tracer;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class LogLineParser {
    private static final Logger logger = LoggerFactory.getLogger(LogLineParser.class);

    public static final long NULL_SPAN = strToLong("null->");
    private static final short ARROW = (short) strToLong("->");
    private static final char LINE_END = '\n';
    private static final int[] DAYS_IN_MONTH = sum(
            0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
    public static final byte[] ZERO_TIME_BYTES = "0000-00-00 00:00:00.000".getBytes();

    private final Dictionary serviceDictionary;
    private final RequestRepo requestRepo;
    private boolean searchNewLine;

    void setSearchNewLine(boolean searchNewLine) {
        this.searchNewLine = searchNewLine;
    }

    boolean isSearchNewLine() {
        return searchNewLine;
    }

    public LogLineParser(Dictionary serviceDictionary, RequestRepo requestRepo) {
        this.serviceDictionary = serviceDictionary;
        this.requestRepo = requestRepo;
    }

    // 2013-10-23T10:13:04.978Z 2013-10-23T10:13:04.989Z fmpezpru service7 tiaka23p->t4kytvis
    public long parse(ByteBuffer in) {
        while (searchNewLine) {
            for (int i = in.remaining(); i > 0; --i) {
                if (in.get() == '\n') {
                    searchNewLine = false;
                    return 0;
                }
            }
            return 0;
        }
        final long started = readTimeStamp(in);
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final long ended = readTimeStamp(in);
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final long requestId = in.getLong();
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final ByteBuffer serviceName = readToken(in);
        if (serviceName == null) {
            return 0;
        }
        long callerSpan = in.getLong();
        if ((callerSpan & (~0L >>> 16)) == NULL_SPAN) {
            callerSpan &= ~0L >>> 16;
            in.position(in.position() - 2);
        } else {
            final short arrow = in.getShort();
            if (arrow != ARROW) {
                searchNewLine = true;
                return 0;
            }
        }
        final long span = in.getLong();
        if (LINE_END == in.get()) {
            requestRepo.line(serviceDictionary.add(serviceName),
                    requestId, started, ended, callerSpan, span);
        }
        return ended;
    }

    private static long strToLong(String s) {
        long result = 0;
        for (int i = s.length() - 1; i >= 0 ; --i) {
            result = (result << 8) + s.charAt(i);
        }
        return result;
    }

    private ByteBuffer readToken(ByteBuffer in) {
        int start = in.position();
        while (true) {
            int b = in.get();
            if (b == ' ') {
                break;
            } else if (b == LINE_END) {
                searchNewLine = true;
                return null;
            }
        }
        int end = in.position();
        byte [] token = new byte[end - start - 1];
        in.position(start);
        in.get(token, 0, token.length);
        in.position(end); // space
        return ByteBuffer.wrap(token);
    }

    // 2013-10-23T10:13:04.945Z
    public static long readTimeStamp(ByteBuffer in) {
        final int year = parseYear(in);
        return yearsToMs(year)
                + monthsToMs(parseMonth(in), year)
                + DAYS.toMillis(parseDay(in))
                + HOURS.toMillis(parseHours(in))
                + MINUTES.toMillis(parseMinutes(in))
                + SECONDS.toMillis(parseSeconds(in))
                + parseMs(in);
    }

    private static int parseYear(ByteBuffer in) {
        return parseInt(in.getInt());
    }

    // -10-
    private static int parseMonth(ByteBuffer in) {
        return parseHours(in);
    }

    // 23
    private static int parseDay(ByteBuffer in) {
        return parseInt(in.getShort()) - 1;
    }

    private static int parseInt(int text) {
        int result = 0;
        while (text > 0) {
            result = result * 10 + (0xff & text) - '0';
            text >>>= 8;
        }
        return result;
    }

    private static long yearsToMs(int years) {
        return YEAR_TO_MS[years - BASE_YEAR];
    }

    private static long monthsToMs(int month, int year) {
        int days = DAYS_IN_MONTH[month - 1];
        if (leapyear(year) && month > 2) {
            ++days;
        }
        return DAYS.toMillis(days);
    }

    private static int[] sum(int... a) {
        int s = 0;
        for (int i = 0; i < a.length; ++i) {
            s += a[i];
            a[i] = s;
        }
        return a;
    }

    // T10:
    private static int parseHours(ByteBuffer in) {
        return parseInt((in.getInt() >>> 8) & 0xffff);
    }

    // 13
    private static int parseMinutes(ByteBuffer in) {
        return parseInt(in.getShort());
    }

    // :04.
    private static int parseSeconds(ByteBuffer in) {
        return parseHours(in);
    }

    // 945Z
    private static int parseMs(ByteBuffer in) {
        return parseInt(in.getInt() & (~0 >>> 8));
    }

    private static boolean leapyear(int year) {
        return (year) % 4 == 0
                && (((year) % 100) > 0
                || (year) % 400 == 0);
    }

    private static long MSECS_DAY = 24L * 60L * 60L * 1000L;
    private static final LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

    private static final int DAYS_PER_MONTH[] = new int[] {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
            31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    private static final int BASE_YEAR = 1970;
    private static final int[] DAY_TO_YEAR_MONTH;
    private static final long[] YEAR_TO_MS;

    static  {
        final int stopYear = now.getYear() + 2;
        DAY_TO_YEAR_MONTH = new int[(stopYear - BASE_YEAR) * 366];
        YEAR_TO_MS = new long[stopYear - BASE_YEAR];
        int dayno = 0;
        long yearMs = 0;
        for (int year = 1970; year < stopYear; ++year) {
            int leapShift = leapyear(year) ? 12 : 0;
            for (int month = 0; month < 12; ++month) {
                final int days = DAYS_PER_MONTH[month + leapShift];
                for (int day = 1; day <= days; ++day) {
                    YEAR_TO_MS[year - BASE_YEAR] = yearMs;
                    DAY_TO_YEAR_MONTH[dayno] = year << 9 | (day << 4) | (month + 1);
                    ++dayno;
                }
            }
            yearMs += (leapShift / 12L + 365L) * MSECS_DAY;
        }
    }

    // 2013-10-23 10:13:04.945
    public static void writeDateTime(ByteBuffer outputBuf, long time) {
        final int dayNo = (int) (time / MSECS_DAY);
        if (dayNo < 0 || dayNo >= DAY_TO_YEAR_MONTH.length) {
            logger.error("Timestamp {} is out of range", time);
            outputBuf.put(ZERO_TIME_BYTES);
        } else {
            writeDatePart(outputBuf, DAY_TO_YEAR_MONTH[dayNo]);
            writeTimePart(outputBuf, time);
        }
    }

    private static void writeTimePart(ByteBuffer outputBuf, long time) {
        int dayClock = (int) (time % MSECS_DAY);
        final int ms = dayClock % 1000;
        dayClock /= 1000L;
        {
            final int hour = dayClock / 3600;
            writeIntAsStr(outputBuf, hour, 2);
            outputBuf.put((byte) ':');
        }
        {
            final int min = (dayClock % 3600) / 60;
            writeIntAsStr(outputBuf, min, 2);
            outputBuf.put((byte) ':');
        }
        {
            final int sec = dayClock % 60;
            writeIntAsStr(outputBuf, sec, 2);
        }
        outputBuf.put((byte) '.');
        writeIntAsStr(outputBuf, ms, 3);
    }

    private static void writeDatePart(ByteBuffer outputBuf, int i) {
        final int yearDayMonth = i;
        final int year = yearDayMonth >>> 9;
        writeIntAsStr(outputBuf, year, 4);
        outputBuf.put((byte) '-');

        final int month = yearDayMonth & 0xf;
        writeIntAsStr(outputBuf, month, 2);
        outputBuf.put((byte) '-');

        final int day = (yearDayMonth >>> 4) & 0x1f;
        writeIntAsStr(outputBuf, day, 2);
        outputBuf.put((byte) ' ');
    }

    private static void writeIntAsStr(ByteBuffer outputBuf, int n, int width) {
        int pos = outputBuf.position() + width;
        outputBuf.position(pos);
        while (true) {
            outputBuf.put(--pos, (byte) ('0' + (n % 10)));
            n /= 10;
            if (--width <= 0) {
                break;
            }
        }
    }

    public static String timeToString(long time) {
        ByteBuffer b = ByteBuffer.allocate(ZERO_TIME_BYTES.length);
        writeDateTime(b, time);
        return new String(b.array());
    }
}
