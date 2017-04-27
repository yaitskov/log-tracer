package org.dan.tracer;

import static org.dan.tracer.LogLineParser.NULL_SPAN;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

public class Request {
    private static final Logger logger = LoggerFactory.getLogger(Request.class);

    private final Map<Long, Snap> snapMap = new HashMap<>();
    private final long requestId;
    private long oldestLine;

    public Request(long requestId) {
        this.requestId = requestId;
    }

    public Snap getSnap(long id) {
        return snapMap.get(id);
    }

    public long getRequestId() {
        return requestId;
    }

    public int writeAsJson(WritableByteChannel outputCh, ByteBuffer outputBuf,
            Dictionary dictionary) {
        final Snap root = snapMap.get(NULL_SPAN);
        if (root == null) {
            logger.error("Drop request {} without root span", this);
            return 0;
        }
        outputBuf.put("{\"id\":\"".getBytes())
            .putLong(requestId)
            .put("\",root:".getBytes());
        root.writeAsJson(outputCh, outputBuf, dictionary);
        outputBuf.put("}\n".getBytes());
        return 1;
    }

    public void addSnap(Snap snap) {
        snapMap.put(snap.getId(), snap);
    }

    public void updateLastTimeStamp(long last) {
        oldestLine = Math.max(last, oldestLine);
    }

    public long getOldestLine() {
        return oldestLine;
    }

    public void setOldestLine(long oldestLine) {
        this.oldestLine = oldestLine;
    }

    public Map<Long, Snap> getSnapMap() {
        return snapMap;
    }

    public String toString() {
        return Long.toHexString(requestId);
    }
}
