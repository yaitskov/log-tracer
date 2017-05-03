package org.dan.tracer;

import static com.koloboke.collect.map.hash.HashLongObjMaps.newMutableMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.dan.tracer.Converter.FIRST_BLOCK_NO;
import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.koloboke.collect.map.hash.HashLongObjMap;
import org.junit.Before;
import org.junit.Test;

public class RequestRepoTest {
    public static final int SERVICE_ID = 1;
    public static final long REQUEST_ID = 2L;
    public static final int STARTED = 3;
    public static final int ENDED = 13;
    public static final int SNAP_A = 5;
    public static final int SNAP_B = 6;
    public static final long EXPIRE_WINDOW = 100L;
    public static final long BLOCK_TIME_SPAN = 10L * EXPIRE_WINDOW;

    private RequestRepo requestRepo;
    private CommandLineOptions options;
    private Bus bus;

    @Before
    public void setUp() {
        options = new CommandLineOptions();
        options.setExpireRequestAfterMs(EXPIRE_WINDOW);
        bus = new Bus();
        requestRepo = new RequestRepo(null, null, null, options, bus);
    }

    @Test
    public void writeRequestsJsonForwardNoTransitRequests()
            throws InterruptedException {
        Request request = new Request(1);
        request.updateLastTimeStamp(BLOCK_TIME_SPAN - EXPIRE_WINDOW / 2);
        requestRepo.getRequests().put(1L, request);
        final HashLongObjMap<Request> transitRequests = newMutableMap();
        requestRepo.writeRequestsJson(FIRST_BLOCK_NO, transitRequests,
                0L, BLOCK_TIME_SPAN);
        assertTrue(requestRepo.getRequests().isEmpty());
        assertEquals(transitRequests, bus.peekTransitRequests(FIRST_BLOCK_NO));
        assertTrue(transitRequests.isEmpty());
    }

    @Test
    public void writeRequestsJsonForwardTransitRequests() throws InterruptedException {
        final HashLongObjMap<Request> transitRequests = newMutableMap();
        final Request tRequest = new Request(1);
        transitRequests.put(tRequest.getRequestId(), tRequest);
        tRequest.updateLastTimeStamp(BLOCK_TIME_SPAN - EXPIRE_WINDOW / 2L);

        final Request request = new Request(tRequest.getRequestId());
        request.updateLastTimeStamp(2L * BLOCK_TIME_SPAN - EXPIRE_WINDOW / 2L);
        requestRepo.getRequests().put(request.getRequestId(), request);
        requestRepo.writeRequestsJson(FIRST_BLOCK_NO + 1, transitRequests,
                BLOCK_TIME_SPAN, 2L * BLOCK_TIME_SPAN);
        assertTrue(requestRepo.getRequests().isEmpty());
        assertTrue(bus.peekTransitRequests(FIRST_BLOCK_NO)
                .containsKey(tRequest.getRequestId()));
        assertTrue(transitRequests.isEmpty());
    }

    @Test
    public void lineOfNestedTraceComesLast() {
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED, ENDED, NULL_SPAN, SNAP_A);
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED + 1, ENDED - 1, SNAP_A, SNAP_B);
        checkSnapsAB();
    }

    private void checkSnapsAB() {
        Request req = requestRepo.getRequests().get(REQUEST_ID);
        assertEquals(ENDED, req.getNewestLine());

        Span spanA = req.getSpan(SNAP_A);
        Span spanB = req.getSpan(SNAP_B);

        assertEquals(spanB.getChildren(), emptyList());
        assertEquals(spanA.getChildren(), singletonList(spanB));
        assertEquals(spanA.getStarted(), STARTED);
        assertEquals(spanA.getEnded(), ENDED);
        assertEquals(spanA.getServiceId(), SERVICE_ID);
        assertEquals(spanA.getId(), SNAP_A);

        assertEquals(spanB.getId(), SNAP_B);
        assertEquals(spanB.getStarted(), STARTED + 1);
        assertEquals(spanB.getEnded(), ENDED - 1);
        assertEquals(spanB.getServiceId(), SERVICE_ID);
    }

    @Test
    public void lineOfNestedTraceFirst() {
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED + 1, ENDED - 1, SNAP_A, SNAP_B);
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED, ENDED, NULL_SPAN, SNAP_A);
        checkSnapsAB();
    }
}
