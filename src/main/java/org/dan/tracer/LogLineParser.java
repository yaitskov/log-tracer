package org.dan.tracer;

import static java.nio.ByteBuffer.wrap;
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

    private final Dictionary serviceDictionary;
    private final RequestRepo requestRepo;
    private boolean searchNewLine;

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
        final long started = readEpochTimeStamp(in);
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final long ended = readEpochTimeStamp(in);
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final long requestId = in.getLong();
        if (in.get() != ' ') {
            searchNewLine = true;
            return 0;
        }
        final String serviceName = readToken(in);
        if (serviceName == null) {
            return 0;
        }
        long callerSpan = in.getLong();
        if (callerSpan >>> 16 == NULL_SPAN) {
            callerSpan >>>= 16;
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
        for (int i = 0; i < s.length(); ++i) {
            result = result << 8 + s.charAt(i);
        }
        return result;
    }

    private String readToken(ByteBuffer in) {
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
        byte [] token = new byte[end - start];
        in.get(token, start, token.length);
        return new String(token);
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

    private static final long EPOCH = readTimeStamp(wrap("1970-01-01T00:00:00.000Z ".getBytes()));

    public static long readEpochTimeStamp(ByteBuffer in) {
        return readTimeStamp(in) - EPOCH;
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
        return parseInt(in.getShort());
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
        return years * 365L * 24L * 3600L * 1000L
                + DAYS.toMillis((years - 1) >>> 2);
    }

    private static final int[] DAYS_IN_MONTH = sum(
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);

    private static long monthsToMs(int month, int year) {
        int days = DAYS_IN_MONTH[month - 1];
        if ((year & 7) == 6 && month > 2) {
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
        return parseInt(in.getInt() >>> 8);
    }

    private static boolean leapyear(int year) {
        return (year) % 4 != 0
                && (((year) % 100) > 0
                || (year) % 400 != 0);
    }

    private static final int EPOCH_YR = 1970;
    private static long SECS_DAY = 24L * 60L * 60L * 1000L;
    private static final LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

    private static final int DAYS_PER_MONTH[] = new int[] {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
            31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    private static final int BASE_DAY;
    private static final int[] DAY_TO_YEAR_MONTH;

    static  {
        final int baseYear = now.getYear() - 3;
        final int stopYear = now.getYear() + 2;
        DAY_TO_YEAR_MONTH = new int[stopYear - baseYear];
        int baseDay = 0;
        int dayno = 0;
        for (int year = 1970; year < stopYear; ++year) {
            int leapShift = leapyear(year) ? 12 : 0;
            for (int month = 0; month < 12; ++month) {
                final int days = DAYS_PER_MONTH[month + leapShift];
                for (int day = 1; day < days; ++day) {
                    if (year >= baseYear) {
                        DAY_TO_YEAR_MONTH[dayno] = year << 9 | (day << 4) | (month + 1);
                    } else {
                        baseDay = dayno;
                    }
                    ++dayno;
                }
            }
        }
        BASE_DAY = baseDay + 1;
    }

    // 2013-10-23 10:13:04.945
    public static void writeDate(ByteBuffer outputBuf, long time) {
        final int dayno = (int) (time / SECS_DAY);
        final int basedDayNo = dayno - BASE_DAY;
        if (basedDayNo < 0 || basedDayNo >= DAY_TO_YEAR_MONTH.length) {
            logger.error("Timestamp {} is out of range", time);
            outputBuf.put("0000-00-00 00:00:00.000".getBytes());
        } else {
            {
                final int yearDayMonth = DAY_TO_YEAR_MONTH[basedDayNo];
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
            {
                int dayClock = (int) (time % SECS_DAY);
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
        }
    }

    private static void writeIntAsStr(ByteBuffer outputBuf, int n, int width) {
        outputBuf.position(outputBuf.position() + width);
        while (true) {
            outputBuf.put(outputBuf.position() + width, (byte) ('0' + (n % 10)));
            n /= 10;
            if (width-- == 0) {
                break;
            }
        }
    }
}
