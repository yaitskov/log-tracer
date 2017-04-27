package org.dan.tracer;

import static org.dan.tracer.LogLineParser.writeDate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Snap implements Comparable<Snap> {
    private final List<Snap> children = new ArrayList<>();
    private final long id;
    private int serviceId;
    private long started;
    private long ended;
    private Snap parent;

    public Snap(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean equals(Object o) {
        if (o instanceof Snap) {
            return id == ((Snap) o).id;
        }
        return false;
    }

    public int hashCode() {
        return (int) id;
    }

    public int compareTo(Snap other) {
        return Long.compare(started, other.started);
    }

    public void addChild(Snap snap) {
        snap.setParent(this);
        children.add(snap);
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
        outputBuf.put("{\"start\":\"".getBytes());
        writeDate(outputBuf, started);
        outputBuf.put(",\"end\":\"".getBytes());
        writeDate(outputBuf, ended);
        outputBuf.put(",\"service\":\"".getBytes());
        outputBuf.put(serviceDictionary.getById(serviceId).getBytes());
        if (children.isEmpty()) {
            outputBuf.put((byte) '"');
        } else {
            outputBuf.put("\",\"calls\":[".getBytes());
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

    public Snap getParent() {
        return parent;
    }

    public void setParent(Snap parent) {
        this.parent = parent;
    }

    public List<Snap> getChildren() {
        return children;
    }
}
