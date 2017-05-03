package org.dan.tracer;

import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.dan.tracer.Request.Status.MORE_SPACE;
import static org.dan.tracer.Request.Status.OK;
import static org.dan.tracer.Request.Status.SKIP;

import com.koloboke.collect.LongCursor;
import com.koloboke.collect.map.LongObjCursor;
import com.koloboke.collect.map.LongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.LongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Request {
    private static final Logger logger = LoggerFactory.getLogger(Request.class);
    private static final byte[] ID_BYTES = "{\"id\":\"".getBytes();
    private static final byte[] ROOT_BYTES = "\",\"root\":".getBytes();
    private static final byte[] TERMINATOR_BYTES = "}\n".getBytes();
    public static final int MIN_FREE_SPACE_BYTES = 100;

    private final HashLongObjMap<Span> spanMap = HashLongObjMaps.newMutableMap();
    private final LongSet sourceSpans = HashLongSets.newMutableSet();
    private final long requestId;
    private long newestLine;
    private long oldestLine;

    HashLongObjMap<Span> getSpanMap() {
        return spanMap;
    }

    LongSet getSourceSpans() {
        return sourceSpans;
    }

    public long getOldestLine() {
        return oldestLine;
    }

    public long getRequestId() {
        return requestId;
    }

    public Request(long requestId) {
        this.requestId = requestId;
    }

    public Span getSpan(long id) {
        return spanMap.get(id);
    }

    public void removeSourceSpan(long snapId) {
        sourceSpans.removeLong(snapId);
    }

    public enum Status { SKIP, OK, MORE_SPACE }

    public Status writeAsJson(ByteBuffer outputBuf, Dictionary dictionary) {
        final Span root = spanMap.get(NULL_SPAN);
        if (root == null || root.getChildren().isEmpty()) {
            logger.info("Drop request {} without root span", this);
            return SKIP;
        } else if (outputBuf.remaining() < MIN_FREE_SPACE_BYTES) {
            return MORE_SPACE;
        }
        outputBuf.put(ID_BYTES)
            .putLong(requestId)
            .put(ROOT_BYTES);
        if (root.getChildren().get(0).writeAsJson(outputBuf, dictionary) == MORE_SPACE) {
            return MORE_SPACE;
        }
        outputBuf.put(TERMINATOR_BYTES);
        return OK;
    }

    public void addSpan(Span span) {
        spanMap.put(span.getId(), span);
    }

    public void addSourceSpan(Span span) {
        spanMap.put(span.getId(), span);
        sourceSpans.add(span.getId());
    }

    public void updateLastTimeStamp(long last) {
        newestLine = Math.max(last, newestLine);
        if (last > 0) {
            oldestLine = Math.min(last, oldestLine);
        }
    }

    public long getNewestLine() {
        return newestLine;
    }

    public String toString() {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(requestId);
        return new String(b.array());
    }

    public void merge(Request forwarded) {
        updateLastTimeStamp(forwarded.newestLine);
        final LongCursor sourceSpanCursor = sourceSpans.cursor();
        final LongObjMap<Span> forwardedMap = forwarded.spanMap;
        while (sourceSpanCursor.moveNext()) {
            final Span span = spanMap.get(sourceSpanCursor.elem());
            walk(span, forwardedMap);
            final Span sourceFwd = forwardedMap.remove(sourceSpanCursor.elem());
            if (sourceFwd != null) {
                if (sourceFwd.getServiceId() != 0 && span.getServiceId() == 0) {
                    spanMap.put(sourceFwd.getId(), sourceFwd);
                    sourceFwd.mergeChildrenOf(span);
                } else if (sourceFwd.getServiceId() == 0 && span.getServiceId() != 0
                        || sourceFwd.getServiceId() == 0 && span.getServiceId() == 0)  {
                    span.mergeChildrenOf(sourceFwd);
                } else {
                    logger.error("Snap {} is used twice", span.getId());
                }
            }
        }
        final LongObjCursor<Span> fwdCursor = forwardedMap.cursor();
        while (fwdCursor.moveNext()) {
            if (forwarded.sourceSpans.contains(fwdCursor.key())) {
                sourceSpans.add(fwdCursor.key());
            }
            Span span = spanMap.get(fwdCursor.key());
            if (span == null) {
                spanMap.put(fwdCursor.key(), fwdCursor.value());
            } else if (fwdCursor.value().getServiceId() != 0 && span.getServiceId() == 0) {
                spanMap.put(fwdCursor.key(), fwdCursor.value());
                fwdCursor.value().mergeChildrenOf(span);
            } else if (fwdCursor.value().getServiceId() == 0 && span.getServiceId() != 0
                    || fwdCursor.value().getServiceId() == 0 && span.getServiceId() == 0)  {
                span.mergeChildrenOf(fwdCursor.value());
            } else {
                logger.error("Snap {} is used twice", span.getId());
            }
        }
    }

    private void walk(Span span, LongObjMap<Span> forwardedMap) {
        for (Span childA : span.getChildren()) {
            Span forwardedChildA = forwardedMap.remove(childA.getId());
            if (forwardedChildA == null) {
                continue;
            }
            for (Span childB : forwardedChildA.getChildren()) {
                Span b = spanMap.get(childB.getId());
                if (b == null) {
                    spanMap.put(childB.getId(), childB);
                } else {
                    childB.mergeChildrenOf(b);
                    spanMap.put(childB.getId(), childB);
                }
            }
        }
    }
}
