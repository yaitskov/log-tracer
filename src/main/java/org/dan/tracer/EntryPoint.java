package org.dan.tracer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class EntryPoint {
    private static final int CHECK_AUTO_END_AFTER_LINES = 10000;

    public static void main(final String[] args) throws IOException {
        final CommandLineOptions options = new CommandLineOptions();
        options.parse(args);
        final ReadableByteChannel inputCh = options.getInputCh();
        final WritableByteChannel outputCh = options.getOutputCh();
        final Dictionary serviceDictionary = Dictionary.create();
        final ByteBuffer outputBuf = ByteBuffer.allocate(options.getWriteBufferBytes())
                .order(BIG_ENDIAN);
        final RequestRepo requestRepo = new RequestRepo(
                serviceDictionary, outputCh, outputBuf);
        final LogLineParser logLineParser = new LogLineParser(
                serviceDictionary, requestRepo);
        final ByteBuffer inputBuf = ByteBuffer.allocate(options.getReadBufferBytes())
                .order(LITTLE_ENDIAN);

        int linesSinceAutoEnd = 0;
        long oldestTime = 0;
        try {
            while (true) {
                final int read = inputCh.read(inputBuf);
                if (read < 0) {
                    break;
                }
                inputBuf.flip();
                while (inputBuf.remaining() > options.getMaxLineLength()) {
                    oldestTime = Math.max(oldestTime, logLineParser.parse(inputBuf));
                    ++linesSinceAutoEnd;
                }
                inputBuf.compact();
                if (linesSinceAutoEnd > options.getFlushLineCheck()) {
                    requestRepo.autoEnd(oldestTime, options.getExpireRequestAfterMs());
                    linesSinceAutoEnd = 0;
                }
            }
            requestRepo.autoEnd(oldestTime, 0);
        } finally {
            outputCh.write(outputBuf);
            outputCh.close();
        }
    }
}
