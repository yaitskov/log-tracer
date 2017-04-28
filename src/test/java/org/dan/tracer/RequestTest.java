package org.dan.tracer;

import static org.dan.tracer.LogLineParserTest.asLong;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RequestTest {
    public static final String REQUEST_ID = "12345678";
    public static final long SNAP_A = asLong("abcdefgh");
    public static final int SERVICE1_ID = 1;
    private Dictionary dictionary;

    @Before
    public void setUp() {
        dictionary = Dictionary.create();
        dictionary.add("service1");
        dictionary.add("service2");
    }

    @Test
    public void writeJsonEmpty() {
        Request request = new Request(asLong(REQUEST_ID));
        Snap rootSnap = new Snap(LogLineParser.NULL_SPAN);
        request.addSnap(rootSnap);
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0, request.writeAsJson(null, buffer, dictionary));
        assertEquals(0, buffer.position());
    }

    @Test
    public void writeJson1() {
        Request request = new Request(asLong(REQUEST_ID));
        Snap rootSnap = new Snap(LogLineParser.NULL_SPAN);
        Snap subSnap = new Snap(SNAP_A);
        subSnap.setStarted(1);
        subSnap.setEnded(3);
        subSnap.setServiceId(SERVICE1_ID);
        rootSnap.addChild(subSnap);
        request.addSnap(rootSnap);
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(1, request.writeAsJson(null, buffer, dictionary));
        buffer.flip();
        assertEquals("{\"id\":\"" + REQUEST_ID + "\",\"root\":{\"start\":\""
                        + "1970-01-01 00:00:00.001\",\"end\":\"1970-01-01 00:00:00.003\","
                        + "\"service\":\"service1\"}}\n",
                new String(buffer.array(), 0, buffer.limit()));
    }
}
