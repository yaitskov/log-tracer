package org.dan.tracer;

import static java.nio.ByteOrder.BIG_ENDIAN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;

public class EntryPoint {
    private static final int MIN_LENGTH_VALID_LINE_BYTES = 83; // plus terminator
    private static final int INPUT_BUFFER_SIZE_BYTES = 8192;
    private static final int OUTPUT_BUFFER_SIZE_BYTES = 8192;
    private static final int CHECK_AUTO_END_AFTER_LINES = 10000;
    private static final long AUTO_END_MS = TimeUnit.SECONDS.toMillis(30);

    public static void main(final String[] args) throws IOException {
        final ReadableByteChannel inputCh = Channels.newChannel(System.in);
        final WritableByteChannel outputCh = Channels.newChannel(System.out);
        final Dictionary serviceDictionary = Dictionary.create();
        final ByteBuffer outputBuf = ByteBuffer.allocate(OUTPUT_BUFFER_SIZE_BYTES)
                .order(BIG_ENDIAN);
        final RequestRepo requestRepo = new RequestRepo(
                serviceDictionary, outputCh, outputBuf);
        final LogLineParser logLineParser = new LogLineParser(
                serviceDictionary, requestRepo);

        final ByteBuffer inputBuf = ByteBuffer.allocate(INPUT_BUFFER_SIZE_BYTES)
                .order(BIG_ENDIAN);

        int linesSinceAutoEnd = 0;
        long oldestTime = 0;
        while (true) {
            final int read = inputCh.read(inputBuf);
            if (read < 0) {
                break;
            }
            inputBuf.flip();
            while (inputBuf.remaining() > MIN_LENGTH_VALID_LINE_BYTES) {
                oldestTime = Math.max(oldestTime, logLineParser.parse(inputBuf));
                ++linesSinceAutoEnd;
            }
            inputBuf.compact();
            if (linesSinceAutoEnd > CHECK_AUTO_END_AFTER_LINES) {
                requestRepo.autoEnd(oldestTime, AUTO_END_MS);
                linesSinceAutoEnd = 0;
            }
        }
        requestRepo.autoEnd(oldestTime, 0);
        outputCh.write(outputBuf);
    }
}
