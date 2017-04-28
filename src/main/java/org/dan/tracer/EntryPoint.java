package org.dan.tracer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class EntryPoint {
    public static void main(final String[] args) throws IOException {
        final CommandLineOptions options = new CommandLineOptions();
        options.parse(args);
        final ReadableByteChannel inputCh = options.getInputCh();
        final WritableByteChannel outputCh = options.getOutputCh();
        final Dictionary serviceDictionary = Dictionary.create();
        final ByteBuffer outputBuf = ByteBuffer.allocate(options.getWriteBufferBytes())
                .order(LITTLE_ENDIAN);
        final RequestRepo requestRepo = new RequestRepo(
                serviceDictionary, outputCh, outputBuf);
        final LogLineParser logLineParser = new LogLineParser(
                serviceDictionary, requestRepo);
        final ByteBuffer inputBuf = ByteBuffer.allocate(options.getReadBufferBytes())
                .order(LITTLE_ENDIAN);

        reconstructTraces(options, inputCh, outputCh, outputBuf,
                requestRepo, logLineParser, inputBuf);
    }

    public static void reconstructTraces(CommandLineOptions options,
            ReadableByteChannel inputCh, WritableByteChannel outputCh,
            ByteBuffer outputBuf, RequestRepo requestRepo,
            LogLineParser logLineParser,
            ByteBuffer inputBuf) throws IOException {
        int linesSinceAutoEnd = 0;
        long oldestTime = 0;
        try {
            while (true) {
                final int read = inputCh.read(inputBuf);
                inputBuf.flip();
                if (read < 0) {
                    while (inputBuf.remaining() > 74) {
                        logLineParser.parse(inputBuf);
                    }
                    break;
                }
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
