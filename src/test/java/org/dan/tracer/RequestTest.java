package org.dan.tracer;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.primitives.Longs.asList;
import static com.koloboke.collect.set.hash.HashLongSets.newImmutableSet;
import static java.util.stream.Collectors.toList;
import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.dan.tracer.LogLineParser.strToLong;
import static org.dan.tracer.LogLineParserTest.asLong;
import static org.dan.tracer.Request.Status.OK;
import static org.dan.tracer.Request.Status.SKIP;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class RequestTest {
    public static final String REQUEST_ID = "12345678";
    public static final long SNAP_A = asLong("abcdefgh");
    public static final long SNAP_B = asLong("zxydefgh");
    public static final long SNAP_C = asLong("zxyklmnh");

    public static final int SERVICE1_ID = 1;
    public static final int SERVICE2_ID = 2;
    public static final int SPAN_A_STARTED = 3;
    public static final int SPAN_A_ENDED = 18;
    public static final int SPAN_A_SERVICE = 3;
    public static final int SPAN_B_STARTED = 5;
    public static final int SPAN_B_ENDED = 17;
    public static final int SPAN_B_SERVICE = 4;
    private Dictionary dictionary;

    @Before
    public void setUp() {
        dictionary = Dictionary.create();
        dictionary.add(ByteBuffer.wrap("service1".getBytes()));
        dictionary.add(ByteBuffer.wrap("service2".getBytes()));
    }

    @Test
    public void writeJsonEmpty() {
        Request request = new Request(asLong(REQUEST_ID));
        Span rootSpan = new Span(NULL_SPAN);
        request.addSpan(rootSpan);
        ByteBuffer buffer = allocate();
        assertEquals(SKIP, request.writeAsJson(buffer, dictionary));
        assertEquals(0, buffer.position());
    }

    private ByteBuffer allocate() {
        return ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void writeJsonLevel1() {
        Request request = new Request(asLong(REQUEST_ID));
        Span rootSpan = new Span(NULL_SPAN);
        Span subSpan = new Span(SNAP_A);
        subSpan.setStarted(1);
        subSpan.setEnded(3);
        subSpan.setServiceId(SERVICE1_ID);
        rootSpan.addChild(subSpan);
        request.addSpan(rootSpan);
        ByteBuffer buffer = allocate();
        assertEquals(OK, request.writeAsJson(buffer, dictionary));
        buffer.flip();
        assertEquals("{\"id\":\"" + REQUEST_ID + "\",\"root\":{\"start\":\""
                        + "1970-01-01 00:00:00.001\",\"end\":\"1970-01-01 00:00:00.003\","
                        + "\"service\":\"service1\"}}\n",
                new String(buffer.array(), 0, buffer.limit()));
    }

    @Test
    public void writeJsonLevel2() {
        Request request = new Request(asLong(REQUEST_ID));
        Span rootSpan = new Span(NULL_SPAN);
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
        request.addSpan(rootSpan);
        ByteBuffer buffer = allocate();
        assertEquals(OK, request.writeAsJson(buffer, dictionary));
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
        Span rootSpan = new Span(NULL_SPAN);
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
            Span child3 = new Span(SNAP_C);
            child3.setStarted(8);
            child3.setEnded(9);
            child3.setServiceId(SERVICE2_ID);
            subSpan.addChild(child3);
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
        request.addSpan(rootSpan);
        ByteBuffer buffer = allocate();
        assertEquals(OK, request.writeAsJson(buffer, dictionary));
        buffer.flip();
        assertEquals("{\"id\":\"" + REQUEST_ID + "\",\"root\":{\"start\":\""
                        + "1970-01-01 00:00:00.001\",\"end\":\"1970-01-01 00:00:00.003\","
                        + "\"service\":\"service1\",\"calls\":["
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.004\",\"end\":\"1970-01-01 00:00:00.005\","
                        + "\"service\":\"service2\"},"
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.005\",\"end\":\"1970-01-01 00:00:00.007\","
                        + "\"service\":\"service2\"},"
                        + "{\"start\":\""
                        + "1970-01-01 00:00:00.008\",\"end\":\"1970-01-01 00:00:00.009\","
                        + "\"service\":\"service2\"}]}}\n",
                new String(buffer.array(), 0, buffer.limit()));
    }

    @Test
    public void mergeChildTransitAndRoot() {
        final Request transit = new Request(strToLong("req1"));
        transit.updateLastTimeStamp(17);
        final Span transitSpanA = new Span(strToLong("spanA"));
        transit.addSourceSpan(transitSpanA);
        final Span spanB = new Span(strToLong("spanB"));
        spanB.setStarted(SPAN_B_STARTED);
        spanB.setServiceId(SPAN_B_SERVICE);
        spanB.setEnded(SPAN_B_ENDED);
        transitSpanA.addChild(spanB);
        transit.addSpan(spanB);
        final Request request = new Request(transit.getRequestId());
        final Span rootSpan = new Span(NULL_SPAN);
        request.addSourceSpan(rootSpan);
        final Span spanA = new Span(transitSpanA.getId());
        spanA.setStarted(SPAN_A_STARTED);
        spanA.setEnded(SPAN_A_ENDED);
        spanA.setServiceId(SPAN_A_SERVICE);
        request.addSpan(spanA);
        request.updateLastTimeStamp(18);
        rootSpan.addChild(spanA);

        request.merge(transit);
        assertEquals(17, request.getOldestLine());
        assertEquals(18, request.getNewestLine());
        assertEquals(newImmutableSet(asList(NULL_SPAN)), request.getSourceSpans());
        assertEquals(newHashSet(NULL_SPAN, spanA.getId(), spanB.getId()),
                request.getSpanMap().keySet());
        assertEquals(asList(spanB.getId()), ids(request.getSpan(spanA.getId()).getChildren()));
        assertEquals(asList(), ids(request.getSpan(spanB.getId()).getChildren()));
        assertEquals(asList(spanA.getId()), ids(request.getSpan(rootSpan.getId()).getChildren()));
        assertEquals(SPAN_A_STARTED, request.getSpan(spanA.getId()).getStarted());
        assertEquals(SPAN_A_ENDED, request.getSpan(spanA.getId()).getEnded());
        assertEquals(SPAN_A_SERVICE, request.getSpan(spanA.getId()).getServiceId());

        assertEquals(SPAN_B_STARTED, request.getSpan(spanB.getId()).getStarted());
        assertEquals(SPAN_B_ENDED, request.getSpan(spanB.getId()).getEnded());
        assertEquals(SPAN_B_SERVICE, request.getSpan(spanB.getId()).getServiceId());
    }

    private static List<Long> ids(List<Span> spans) {
        return spans.stream().map(Span::getId).collect(toList());
    }

    @Test
    public void mergeRootTransitAndChild() {
    }

    @Test
    public void mergeSiblingTransitAndSibling() {
    }

    @Test
    public void mergeChildTransitAndRootWithSubChild() {
    }

    @Test
    public void mergeRootWithSubChildTransitAndRootChild() {
    }

    @Test
    public void mergeRootWithChildTransitAndSibling() {
    }

    @Test
    public void mergeSiblingTransitAndRootWithChild() {
    }
}
