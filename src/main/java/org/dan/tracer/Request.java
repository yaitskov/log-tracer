package org.dan.tracer;

import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.dan.tracer.Span.ensureSpace;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class Request {
    private static final Logger logger = LoggerFactory.getLogger(Request.class);
    private static final byte[] ID_BYTES = "{\"id\":\"".getBytes();
    private static final byte[] ROOT_BYTES = "\",\"root\":".getBytes();
    private static final byte[] TERMINATOR_BYTES = "}\n".getBytes();

    private final HashLongObjMap<Span> snapMap = HashLongObjMaps.newMutableMap();
    private final long requestId;
    private long oldestLine;

    public Request(long requestId) {
        this.requestId = requestId;
    }

    public Span getSnap(long id) {
        return snapMap.get(id);
    }

    public int writeAsJson(WritableByteChannel outputCh, ByteBuffer outputBuf,
            Dictionary dictionary) {
        final Span root = snapMap.get(NULL_SPAN);
        if (root == null || root.getChildren().isEmpty()) {
            logger.info("Drop request {} without root span", this);
            return 0;
        }
        ensureSpace(100, outputCh, outputBuf);
        outputBuf.put(ID_BYTES)
            .putLong(requestId)
            .put(ROOT_BYTES);
        root.getChildren().get(0).writeAsJson(outputCh, outputBuf, dictionary);
        outputBuf.put(TERMINATOR_BYTES);
        return 1;
    }

    public void addSnap(Span span) {
        snapMap.put(span.getId(), span);
    }

    public void updateLastTimeStamp(long last) {
        oldestLine = Math.max(last, oldestLine);
    }

    public long getOldestLine() {
        return oldestLine;
    }

    public String toString() {
        ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(requestId);
        return new String(b.array());
    }
}
