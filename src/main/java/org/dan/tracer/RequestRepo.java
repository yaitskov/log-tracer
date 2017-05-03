package org.dan.tracer;

import static com.koloboke.collect.map.hash.HashLongObjMaps.newMutableMap;
import static org.slf4j.LoggerFactory.getLogger;

import com.koloboke.collect.map.LongObjCursor;
import com.koloboke.collect.map.hash.HashLongObjMap;
import org.dan.tracer.Request.Status;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class RequestRepo {
    private static final Logger logger = getLogger(RequestRepo.class);

    private final BlockingQueue<ByteBuffer> outputBackQueue;
    private final BlockingQueue<ByteBuffer> outputQueue;
    private final Dictionary serviceDictionary;
    private final CommandLineOptions options;
    private final Bus bus;

    private ByteBuffer outputBuf;
    private final HashLongObjMap<Request> requests = newMutableMap();
    private int maxSiblings;
    private int blockParsed;
    private long bytesWritten;
    private int blocksWritten;
    private int entriesComplete;
    private int entriesSkipped;
    private int linesParsed;
    private int linesDropped;
    private long lastStatLogAt;

    public RequestRepo(BlockingQueue<ByteBuffer> outputBackQueue,
            BlockingQueue<ByteBuffer> outputQueue,
            Dictionary serviceDictionary,
            CommandLineOptions options, Bus bus) {
        this.outputBackQueue = outputBackQueue;
        this.outputQueue = outputQueue;
        this.serviceDictionary = serviceDictionary;
        this.options = options;
        this.bus = bus;
        lastStatLogAt = System.currentTimeMillis();
    }

    public void line(int serviceId, long requestId, long started,
            long ended, long callerSnapId, long snapId) {
        ++linesParsed;
        Request request = requests.get(requestId);
        if (request == null) {
            request = new Request(requestId);
            requests.put(requestId, request);
        }
        request.updateLastTimeStamp(ended);
        Span callerSpan = request.getSpan(callerSnapId);
        if (callerSpan == null) {
            request.addSourceSpan(callerSpan = new Span(callerSnapId));
        }
        Span span = request.getSpan(snapId);
        if (span == null) {
            request.addSpan(span = new Span(snapId));
        } else {
            request.removeSourceSpan(snapId);
        }
        span.setServiceId(serviceId);
        span.setStarted(started);
        span.setEnded(ended);
        maxSiblings = Math.max(maxSiblings, callerSpan.addChild(span));
    }

    public void writeRequestsJson(final int blockId,
            HashLongObjMap<Request> transitRequests,
            final long oldestTime, final long newestTime) throws InterruptedException {
        int complete = 0;
        final long transitWindow = options.getExpireRequestAfterMs();
        final long transitAfter = newestTime - transitWindow;
        final long flushAfter = oldestTime + transitAfter;
        final LongObjCursor<Request> iter = requests.cursor();
        final HashLongObjMap<Request> forwardRequests = newMutableMap();
        while (iter.moveNext()) {
            final Request request = iter.value();
            final long newestLine = request.getNewestLine();
            if (newestLine > transitAfter) {
                Request forwardReqPart = transitRequests.remove(iter.key());
                if (forwardReqPart != null) {
                    request.merge(forwardReqPart);
                }
                forwardRequests.put(iter.key(), request);
            } else if (request.getOldestLine() > flushAfter) {
                complete += writeRequestAsJson(request);
            } else {
                complete += mergeAndFlush(request, transitRequests);
            }
        }
        complete += forwardAndFlush(blockId, forwardRequests, transitRequests);
        logStats(complete);
    }

    private int forwardAndFlush(int blockId, HashLongObjMap<Request> forwardRequests,
            HashLongObjMap<Request> transitRequests) throws InterruptedException {
        final boolean lastBlock = isLastBlock(blockId);
        if (!lastBlock) {
            bus.putTransitRequests(blockId, forwardRequests);
        }
        int complete = flushRequests(transitRequests);
        if (lastBlock) {
            complete += flushRequests(forwardRequests);
        }
        return complete;
    }

    private static boolean isLastBlock(int blockId) {
        return blockId < 0;
    }

    private int flushRequests(HashLongObjMap<Request> transitRequests)
            throws InterruptedException {
        final LongObjCursor<Request> iter = transitRequests.cursor();
        int counter = 0;
        while (iter.moveNext()) {
            counter += writeRequestAsJson(iter.value());
        }
        return counter;
    }

    private int mergeAndFlush(final Request request,
            final HashLongObjMap<Request> transitRequests)
            throws InterruptedException {
        final Request forwardReqPart = transitRequests.remove(request.getRequestId());
        if (forwardReqPart != null) {
            request.merge(forwardReqPart);
        }
        return writeRequestAsJson(request);
    }

    private int writeRequestAsJson(Request request) throws InterruptedException {
        while (true) {
            final int firstByteOfEntry = outputBuf.position();
            final Status status = request.writeAsJson(outputBuf, serviceDictionary);
            switch (status) {
                case OK:
                    return 1;
                case MORE_SPACE:
                    if (firstByteOfEntry == 0) {
                        throw new IllegalStateException(
                                "Output buffer size of " + outputBuf.capacity()
                                        + " is not small to fit biggest entry."
                                        + " Use option -wbuf to increase buffer size.");
                    }
                    bytesWritten += firstByteOfEntry;
                    ++blocksWritten;
                    outputBuf.position(firstByteOfEntry);
                    outputBuf.flip();
                    outputQueue.put(outputBuf);
                    outputBuf = outputBackQueue.take();
                    outputBuf.clear();
                    break;
                case SKIP:
                    ++entriesSkipped;
                    return 0;
                default:
                    throw new IllegalStateException("status "
                            + status + " is not supported");
            }
        }
    }

    public void logStats(int complete) {
        entriesComplete += complete;
        if (logger.isInfoEnabled() && ++blockParsed > options.getLogStatsAfterBlocks()) {
            final long now = System.currentTimeMillis();
            final double duration = Math.max(1L, now - lastStatLogAt);
            lastStatLogAt = now;
            logger.info("Max span siblings {}", maxSiblings);
            logger.info("Entries complete {} / skipped {}",
                    entriesComplete, entriesSkipped);
            final int bytesRead = blockParsed * options.getReadBufferBytes();
            logger.info("Blocks read {} (~{} bytes) / written {} ({} bytes)",
                    blockParsed, bytesRead, blocksWritten, bytesWritten);
            logger.info("Speed read {} / write {} MB/s",
                    bytesRead * 1000.0 / duration,
                    blocksWritten * 1000.0 / duration);
            logger.info("Lines accepted {} / dropped {}", linesParsed, linesDropped);
            blockParsed = 0;
            bytesWritten = 0;
            blocksWritten = 0;
            entriesComplete = 0;
            entriesSkipped = 0;
            linesDropped = 0;
            linesParsed = 0;
        }
    }

    Map<Long, Request> getRequests() {
        return requests;
    }

    public void setOutputBuf(ByteBuffer outputBuf) {
        this.outputBuf = outputBuf;
    }

    public ByteBuffer getOutputBuf() {
        return outputBuf;
    }

    public void dropLine() {
        ++linesDropped;
    }
}
