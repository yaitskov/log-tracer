package org.dan.tracer;

import static org.dan.tracer.Bus.getWorkerThreads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.BlockingQueue;

public class BlockWriter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BlockWriter.class);
    public static final ByteBuffer EXIT = ByteBuffer.allocate(1);

    private final BlockingQueue<ByteBuffer> outputQueue;
    private final BlockingQueue<ByteBuffer> outputBackQueue;
    private final WritableByteChannel outputChannel;

    public BlockWriter(
            BlockingQueue<ByteBuffer> outputQueue,
            BlockingQueue<ByteBuffer> outputBackQueue,
            WritableByteChannel outputChannel) {
        this.outputQueue = outputQueue;
        this.outputBackQueue = outputBackQueue;
        this.outputChannel = outputChannel;
    }

    public void run() {
        logger.info("Block writer started");
        long writtenBytes = 0;
        int writtenBlocks = 0;
        int exitBlocks = getWorkerThreads();
        try {
            while (true) {
                ByteBuffer buf = outputQueue.take();
                if (buf == EXIT && --exitBlocks <= 0) {
                    break;
                }
                ++writtenBlocks;
                writtenBytes += outputChannel.write(buf);
                outputBackQueue.put(buf);
            }
        } catch (InterruptedException e) {
            logger.error("Writer thread is interrupted", e);
        } catch (IOException e) {
            logger.error("Failed to write block", e);
        } finally {
            logger.info("Blocked writer terminated. Written {} bytes; blocks",
                    writtenBytes, writtenBlocks);
        }
    }
}
