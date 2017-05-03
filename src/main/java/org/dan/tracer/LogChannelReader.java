package org.dan.tracer;

import static org.dan.tracer.Bus.getWorkerThreads;
import static org.dan.tracer.Converter.FIRST_BLOCK_NO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;

public class LogChannelReader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LogChannelReader.class);

    private final BlockingQueue<ByteBuffer> inputBuffers;
    private final BlockingQueue<Block> workerInput;
    private final ReadableByteChannel inputChannel;
    private final ByteBuffer tail;

    public LogChannelReader(BlockingQueue<ByteBuffer> inputBuffers,
            BlockingQueue<Block> workerInput, ReadableByteChannel inputChannel,
            ByteBuffer tail) {
        this.inputBuffers = inputBuffers;
        this.workerInput = workerInput;
        this.inputChannel = inputChannel;
        this.tail = tail;
    }

    public void run() {
        logger.info("Block reader started");
        int blockNumber = FIRST_BLOCK_NO;
        try {
            while (true) {
                final ByteBuffer inputBuf = inputBuffers.take();
                inputBuf.put(tail);
                tail.clear();
                final int read = inputChannel.read(inputBuf);
                if (read < 0) {
                    logger.debug("End of input stream. Notify Workers.");
                    for (int i= getWorkerThreads(); i > 0; --i) {
                        workerInput.put(Block.EXIT);
                    }
                } else {
                    inputBuf.flip();
                    int l = findEndOfLastCompleteLine(blockNumber, inputBuf);
                    copyIncompleteLine(inputBuf, l);
                    workerInput.put(new Block(blockNumber++, inputBuf));
                }
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted exception. Break reading.", e);
        } catch (IOException e) {
            logger.error("Failed to read block {}. Break reading.",
                    blockNumber, e);
        } finally {
            logger.info("Block reader terminated");
        }
    }

    private void copyIncompleteLine(ByteBuffer inputBuf, int l) {
        inputBuf.position(l);
        tail.put(inputBuf);
        inputBuf.position(0);
        inputBuf.limit(l);
    }

    private int findEndOfLastCompleteLine(int blockNumber, ByteBuffer inputBuf) {
        int l = inputBuf.limit();
        while (l > 0 && inputBuf.get(--l) != '\n');
        if (l <= 0 || inputBuf.limit() - l > tail.capacity()) {
            throw new IllegalStateException("Buffer of block "
                    + blockNumber + "doesn't have any line.");
        }
        return l + 1;
    }
}
