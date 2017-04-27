package org.dan.tracer;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class CommandLineOptions {
    private static final Logger logger = LoggerFactory.getLogger(CommandLineOptions.class);
    private long expireRequestAfterMs = MINUTES.toMillis(3);
    private int readBufferBytes = 8192;
    private int writeBufferBytes = 8192;
    private int maxLineLength = 120;
    private int flushLineCheck = 10000;
    private ReadableByteChannel inputCh = Channels.newChannel(System.in);
    private WritableByteChannel outputCh = Channels.newChannel(System.out);

    public int getFlushLineCheck() {
        return flushLineCheck;
    }

    public int getMaxLineLength() {
        return maxLineLength;
    }

    public long getExpireRequestAfterMs() {
        return expireRequestAfterMs;
    }

    public int getReadBufferBytes() {
        return readBufferBytes;
    }

    public int getWriteBufferBytes() {
        return writeBufferBytes;
    }

    public ReadableByteChannel getInputCh() {
        return inputCh;
    }

    public WritableByteChannel getOutputCh() {
        return outputCh;
    }

    public void parse(String[] args) {
        for (int i = 0; i < args .length; ++i) {
            switch (args[i]) {
                case "-in":
                    openInput(optionStringArg(args, i++));
                    break;
                case "-out":
                    openOutput(optionStringArg(args, i++));
                    break;
                case "-help":
                    printHelp();
                    break;
                case "-flush-check":
                    flushLineCheck = optionIntArg(args, i++);
                    break;
                case "-rbuf":
                    readBufferBytes = optionIntArg(args, i++);
                    break;
                case "-max-line":
                    maxLineLength = optionIntArg(args, i++);
                    break;
                case "-wbuf":
                    writeBufferBytes = optionIntArg(args, i++);
                    break;
                case "-expire-minute":
                    expireRequestAfterMs = MINUTES.toMillis(optionIntArg(args, i++));
                    break;
                default:
                    error("Unknown option [{}] among [{}]", args[i],
                            String.join(" ", asList(args)));
            }
        }
    }

    private void openInput(String s) {
        try {
            inputCh = new FileInputStream(s).getChannel();
        } catch (FileNotFoundException e) {
            error("Failed to open file {}", s, e);
        }
    }

    private void openOutput(String s) {
        try {
            inputCh = new FileOutputStream(s).getChannel();
        } catch (FileNotFoundException e) {
            error("Failed to open file {}", s, e);
        }
    }

    private String optionStringArg(String[] args, int i) {
        if (args.length == i) {
            error("Option {} expected file argument", args[i - 1]);
        }
        return args[i];
    }

    private int optionIntArg(String[] args, int i) {
        if (args.length == i) {
            error("Option {} expected integer argument", args[i - 1]);
        }
        try {
            int n = Integer.parseInt(args[i]);
            if (n < 100) {
                error("Option {} should be more 100", n);
            }
            return n;
        } catch (NumberFormatException e) {
            error("Option {} expected integer argument but got {}", args[i - 1], args[i]);
            return -1;
        }
    }

    private void error(String pattern, Object... args) {
        logger.error(pattern, args);
        System.exit(1);
    }

    private void printHelp() {
        logger.info("Usage: tracer [ -in log_file ] [ -out trace_file ] [ ... ]\n"
                + "  Options: \n"
                + "    -rbuf           - size of read buffer in bytes\n"
                + "    -wbuf           - size of write buffer in bytes\n"
                + "    -flush-check    - check expired requests after N log lines are consumed\n"
                + "    -max-line       - max expected length of a log line\n"
                + "    -expire-minute  - max difference for request between \n"
                + "                      the newest line and newer request line\n");
        System.exit(1);
    }
}
