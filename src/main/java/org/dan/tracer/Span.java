package org.dan.tracer;

import static com.google.common.collect.Iterables.mergeSorted;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.dan.tracer.LogLineParser.writeDateTime;
import static org.dan.tracer.Request.MIN_FREE_SPACE_BYTES;
import static org.dan.tracer.Request.Status.MORE_SPACE;
import static org.dan.tracer.Request.Status.OK;

import org.dan.tracer.Request.Status;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Span implements Comparable<Span> {
    private static final byte[] START_BYTES = "{\"start\":\"".getBytes();
    private static final byte[] END_BYTES = "\",\"end\":\"".getBytes();
    private static final byte[] SERVICE_BYTES = "\",\"service\":\"".getBytes();
    private static final byte[] CALLS_BYTES = "\",\"calls\":[".getBytes();

    private List<Span> children = new ArrayList<>();
    private final long id;
    private int serviceId;
    private long started;
    private long ended;

    public Span(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean equals(Object o) {
        if (o instanceof Span) {
            return id == ((Span) o).id;
        }
        return false;
    }

    public int hashCode() {
        return (int) id;
    }

    public int compareTo(Span other) {
        return Long.compare(started, other.started);
    }

    public int addChild(final Span span) {
        final int size = children.size();
        for (int i = size - 1; i >= 0; --i) {
            if (children.get(i).getStarted() < span.getStarted()) {
                children.add(i + 1, span);
                return size + 1;
            }
        }
        children.add(0, span);
        return size + 1;
    }

    public Status writeAsJson(ByteBuffer outputBuf,
            Dictionary serviceDictionary) {
        if (outputBuf.remaining() < MIN_FREE_SPACE_BYTES) {
            return MORE_SPACE;
        }
        outputBuf.put(START_BYTES);
        writeDateTime(outputBuf, started);
        outputBuf.put(END_BYTES);
        writeDateTime(outputBuf, ended);
        outputBuf.put(SERVICE_BYTES);
        outputBuf.put(serviceDictionary.getById(serviceId).array());
        if (children.isEmpty()) {
            outputBuf.put((byte) '"');
        } else {
            outputBuf.put(CALLS_BYTES);
            if (children.get(0).writeAsJson(outputBuf, serviceDictionary) == MORE_SPACE) {
                return MORE_SPACE;
            }
            for (int i = 1; i < children.size(); ++i) {
                outputBuf.put((byte) ',');
                if (children.get(i).writeAsJson(outputBuf, serviceDictionary) == MORE_SPACE) {
                    return MORE_SPACE;
                }
            }
            outputBuf.put((byte) ']');
        }
        outputBuf.put((byte) '}');
        return OK;
    }

    public int getServiceId() {
        return serviceId;
    }

    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }

    public long getStarted() {
        return started;
    }

    public void setStarted(long started) {
        this.started = started;
    }

    public long getEnded() {
        return ended;
    }

    public void setEnded(long ended) {
        this.ended = ended;
    }

    public List<Span> getChildren() {
        return children;
    }

    public void mergeChildrenOf(Span span) {
        children = newArrayList(mergeSorted(asList(children, span.children),
                Span::compareTo));
    }
}
