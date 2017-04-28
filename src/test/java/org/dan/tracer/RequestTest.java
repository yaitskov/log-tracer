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
        Span rootSpan = new Span(LogLineParser.NULL_SPAN);
        request.addSnap(rootSpan);
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0, request.writeAsJson(null, buffer, dictionary));
        assertEquals(0, buffer.position());
    }

    @Test
    public void writeJsonLevel1() {
        Request request = new Request(asLong(REQUEST_ID));
        Span rootSpan = new Span(LogLineParser.NULL_SPAN);
        Span subSpan = new Span(SNAP_A);
        subSpan.setStarted(1);
        subSpan.setEnded(3);
        subSpan.setServiceId(SERVICE1_ID);
        rootSpan.addChild(subSpan);
        request.addSnap(rootSpan);
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
        Span rootSpan = new Span(LogLineParser.NULL_SPAN);
        Span subSpan = new Span(SNAP_A);
        subSpan.setStarted(1);
        subSpan.setEnded(3);
        Span subSpan2 = new Span(SNAP_B);
        subSpan2.setStarted(4);
        subSpan2.setEnded(5);
        subSpan2.setServiceId(SERVICE2_ID);
        subSpan.addChild(subSpan2);
        subSpan.setServiceId(SERVICE1_ID);
        rootSpan.addChild(subSpan);
        request.addSnap(rootSpan);
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

    @Test
    public void writeJson2ChildrenSortByStart() {
        Request request = new Request(asLong(REQUEST_ID));
        Span rootSpan = new Span(LogLineParser.NULL_SPAN);
        Span subSpan = new Span(SNAP_A);
        subSpan.setStarted(1);
        subSpan.setEnded(3);
        {
            Span child2 = new Span(SNAP_B);
            child2.setStarted(5);
            child2.setEnded(7);
            child2.setServiceId(SERVICE2_ID);
            subSpan.addChild(child2);
        }
        {
            Span child1 = new Span(SNAP_B);
            child1.setStarted(4);
            child1.setEnded(5);
            child1.setServiceId(SERVICE2_ID);
            subSpan.addChild(child1);
        }
        subSpan.setServiceId(SERVICE1_ID);
        rootSpan.addChild(subSpan);
        request.addSnap(rootSpan);
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(1, request.writeAsJson(null, buffer, dictionary));
        buffer.flip();
        assertEquals("{\"id\":\"" + REQUEST_ID + "\",\"root\":{\"start\":\""
                        + "1970-01-01 00:00:00.001\",\"end\":\"1970-01-01 00:00:00.003\","
                        + "\"service\":\"service1\",\"calls\":["
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.004\",\"end\":\"1970-01-01 00:00:00.005\","
                        + "\"service\":\"service2\"},"
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.005\",\"end\":\"1970-01-01 00:00:00.007\","
                        + "\"service\":\"service2\"}]}}\n",
                new String(buffer.array(), 0, buffer.limit()));
    }
}
