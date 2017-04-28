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
    public static final long SNAP_B = asLong("zxydefgh");

    public static final int SERVICE1_ID = 1;
    public static final int SERVICE2_ID = 2;
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
    public void writeJsonLevel1() {
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

    @Test
    public void writeJsonLevel2() {
        Request request = new Request(asLong(REQUEST_ID));
        Snap rootSnap = new Snap(LogLineParser.NULL_SPAN);
        Snap subSnap = new Snap(SNAP_A);
        subSnap.setStarted(1);
        subSnap.setEnded(3);
        Snap subSnap2 = new Snap(SNAP_B);
        subSnap2.setStarted(4);
        subSnap2.setEnded(5);
        subSnap2.setServiceId(SERVICE2_ID);
        subSnap.addChild(subSnap2);
        subSnap.setServiceId(SERVICE1_ID);
        rootSnap.addChild(subSnap);
        request.addSnap(rootSnap);
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(1, request.writeAsJson(null, buffer, dictionary));
        buffer.flip();
        assertEquals("{\"id\":\"" + REQUEST_ID + "\",\"root\":{\"start\":\""
                        + "1970-01-01 00:00:00.001\",\"end\":\"1970-01-01 00:00:00.003\","
                        + "\"service\":\"service1\",\"calls\":["
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.004\",\"end\":\"1970-01-01 00:00:00.005\","
                        + "\"service\":\"service2\"}]}}\n",
                new String(buffer.array(), 0, buffer.limit()));
    }
}
