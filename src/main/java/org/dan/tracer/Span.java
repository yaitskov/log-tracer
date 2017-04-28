package org.dan.tracer;

import static org.dan.tracer.LogLineParser.writeDate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Span implements Comparable<Span> {
    private static final byte[] START_BYTES = "{\"start\":\"".getBytes();
    private static final byte[] END_BYTES = "\",\"end\":\"".getBytes();
    private static final byte[] SERVICE_BYTES = "\",\"service\":\"".getBytes();
    private static final byte[] CALLS_BYTES = "\",\"calls\":[".getBytes();

    private final List<Span> children = new ArrayList<>();
    private final long id;
    private int serviceId;
    private long started;
    private long ended;
    private Span parent;

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

    public void addChild(Span span) {
        span.setParent(this);
        children.add(span);
    }

    public static void ensureSpace(final int minFreeSpace,
            final WritableByteChannel outputCh,
            final ByteBuffer outputBuf) {
        if (outputBuf.remaining() < minFreeSpace) {
            outputBuf.flip();
            try {
                outputCh.write(outputBuf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            outputBuf.compact();
        }
    }

    public void writeAsJson(WritableByteChannel outputCh, ByteBuffer outputBuf,
                            Dictionary serviceDictionary) {
        ensureSpace(100, outputCh, outputBuf);
        outputBuf.put(START_BYTES);
        writeDate(outputBuf, started);
        outputBuf.put(END_BYTES);
        writeDate(outputBuf, ended);
        outputBuf.put(SERVICE_BYTES);
        outputBuf.put(serviceDictionary.getById(serviceId).getBytes());
        if (children.isEmpty()) {
            outputBuf.put((byte) '"');
        } else {
            outputBuf.put(CALLS_BYTES);
            Collections.sort(children);
            children.get(0).writeAsJson(outputCh, outputBuf, serviceDictionary);
            for (int i = 1; i < children.size(); ++i) {
                outputBuf.put((byte) ',');
                children.get(i).writeAsJson(outputCh, outputBuf, serviceDictionary);
            }
            outputBuf.put((byte) ']');
        }
        outputBuf.put((byte) '}');
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

    public Span getParent() {
        return parent;
    }

    public void setParent(Span parent) {
        this.parent = parent;
    }

    public List<Span> getChildren() {
        return children;
    }
}
