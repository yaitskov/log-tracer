package org.dan.tracer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.dan.tracer.Bus.getWorkerThreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(EntryPoint.class);

    public static void main(final String[] args) throws IOException {
        final CommandLineOptions options = new CommandLineOptions();
        options.parse(args);
        final ReadableByteChannel inputCh = options.getInputCh();
        final WritableByteChannel outputCh = options.getOutputCh();

        final BlockingQueue<ByteBuffer> outputQueue = emptyQueue();
        final int workerThreads = getWorkerThreads();
        final BlockingQueue<Block> workerInput = emptyQueue();
        final BlockingQueue<ByteBuffer> inputQueue = buffersQueue(workerThreads,
                options.getReadBufferBytes());
        logger.info("start block reader thread");
        new Thread(new LogChannelReader(
                inputQueue, workerInput, inputCh,
                ByteBuffer.allocateDirect(options.getMaxLineLength())),
                "block-reader").start();

        final BlockingQueue<ByteBuffer> outputBackQueue = buffersQueue(workerThreads,
                options.getWriteBufferBytes());
        logger.info("start block writer thread");
        new Thread(new BlockWriter(outputQueue, outputBackQueue, outputCh),
                "block-writer")
                .start();
        final ExecutorService workerPool = Executors.newCachedThreadPool();
        final Bus bus = new Bus();
        logger.info("start {} worker threads", workerThreads);
        final Dictionary globalServiceDictionary = Dictionary.create();
        for (int i = 0; i < workerThreads; ++i) {
            final Dictionary threadServiceDictionary = globalServiceDictionary.fork();
            final RequestRepo requestRepo = new RequestRepo(outputBackQueue, outputQueue,
                    threadServiceDictionary, options, bus);
            workerPool.submit(new Converter(bus, workerInput,
                    outputQueue, outputBackQueue, inputQueue,
                    new LogLineParser(threadServiceDictionary, requestRepo),
                    requestRepo, options));
        }
        logger.info("Init thread terminates");
    }

    private static <T> LinkedBlockingQueue<T> emptyQueue() {
        return new LinkedBlockingQueue<>();
    }

    public static List<ByteBuffer> allocateBuffers(int buffers, int sizeBytes) {
        return IntStream.range(0, buffers)
                .mapToObj(i -> allocateDirect(sizeBytes).order(LITTLE_ENDIAN))
                .collect(Collectors.toList());
    }

    public static BlockingQueue<ByteBuffer> buffersQueue(int buffers, int sizeBytes) {
        return new LinkedBlockingQueue<>(allocateBuffers(buffers, sizeBytes));
    }

}
