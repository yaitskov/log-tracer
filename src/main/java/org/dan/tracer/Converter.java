package org.dan.tracer;

import com.koloboke.collect.map.hash.HashLongObjMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class Converter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Converter.class);
    public static final int FIRST_BLOCK_NO = 0;

    private final Bus bus;
    private final BlockingQueue<Block> workerInput;
    private final BlockingQueue<ByteBuffer> outputQueue;
    private final BlockingQueue<ByteBuffer> outputBackQueue;
    private final BlockingQueue<ByteBuffer> inputBackQueue;
    private final CommandLineOptions options;
    private final RequestRepo requestRepo;

    private LogLineParser logLineParser;

    public Converter(Bus bus, BlockingQueue<Block> workerInput,
            BlockingQueue<ByteBuffer> outputQueue,
            BlockingQueue<ByteBuffer> outputBackQueue,
            BlockingQueue<ByteBuffer> inputBackQueue,
            LogLineParser logLineParser,
            RequestRepo requestRepo,
            CommandLineOptions options) {
        this.bus = bus;
        this.workerInput = workerInput;
        this.outputQueue = outputQueue;
        this.outputBackQueue = outputBackQueue;
        this.inputBackQueue = inputBackQueue;
        this.options = options;
        this.requestRepo = requestRepo;
        this.logLineParser = logLineParser;
    }

    @Override
    public void run() {
        try {
            requestRepo.setOutputBuf(outputBackQueue.take());
            while (true) {
                final Block inBlock = workerInput.take();
                if (inBlock == Block.EXIT) {
                    logger.info("Worker thread got EXIT block. Terminate.");
                    break;
                }
                reconstructTraces(inBlock);
            }
        } catch (InterruptedException e) {
            logger.error("Worker thread was interrupted.");
        } finally {
            if (requestRepo.getOutputBuf() != null) {
                outputQueue.add(requestRepo.getOutputBuf());
            }
            logger.info("Block writer thread is notified about exit {}",
                    outputQueue.add(BlockWriter.EXIT));
        }
    }

    private void reconstructTraces(Block inBlock)
            throws InterruptedException {
        long newestTime = 0;
        long oldestTime = 0;
        long ended;
        try {
            ByteBuffer inputBuf = inBlock.getBuffer();
            while (inputBuf.remaining() > 74) {
                ended = logLineParser.parse(inputBuf);
                newestTime = Math.max(newestTime, ended);
                if (ended > 0) {
                    oldestTime = Math.min(oldestTime, ended);
                }
            }
            final int blockNumber = inBlock.getNumber();
            if (inputBuf.hasRemaining()) {
                logger.error("Waste tailing {} bytes of block {}",
                        inputBuf.hasRemaining(), blockNumber);
            }
            long writeStartingWith = oldestTime;
            HashLongObjMap<Request> transitRequests = null;
            if (blockNumber == FIRST_BLOCK_NO) {
                writeStartingWith = -options.getExpireRequestAfterMs();

            } else {
                transitRequests = bus.getTransitRequests(blockNumber - 1);
            }
            requestRepo.writeRequestsJson(blockNumber, transitRequests,
                    writeStartingWith, newestTime);
        } finally {
            inBlock.getBuffer().clear();
            inputBackQueue.put(inBlock.getBuffer());
        }
    }
}
