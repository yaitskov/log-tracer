package org.dan.tracer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class RequestRepoTest {
    public static final int SERVICE_ID = 1;
    public static final long REQUEST_ID = 2L;
    public static final int STARTED = 3;
    public static final int ENDED = 13;
    public static final int SNAP_A = 5;
    public static final int SNAP_B = 6;

    private RequestRepo requestRepo;

    @Before
    public void setUp() {
        requestRepo = new RequestRepo(null, null, null);
    }

    @Test
    public void autoEndSkip() {
        Request request = new Request(1);
        request.updateLastTimeStamp(100L);
        requestRepo.getRequests().put(1L, request);
        requestRepo.autoEnd(101L, 5);
        assertEquals(1, requestRepo.getRequests().size());
    }

    @Test
    public void autoEndCollects() {
        int[] serialized = new int[1];
        Request request = new Request(1) {
            @Override
            public int writeAsJson(WritableByteChannel outputCh, ByteBuffer outputBuf, Dictionary dictionary) {
                ++serialized[0];
                return 1;
            }
        };
        request.updateLastTimeStamp(100L);
        requestRepo.getRequests().put(1L, request);
        requestRepo.autoEnd(106L, 5);
        assertEquals(0, requestRepo.getRequests().size());
        assertEquals(1, serialized[0]);
    }

    @Test
    public void lineOfNestedTraceComesLast() {
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED, ENDED, NULL_SPAN, SNAP_A);
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED + 1, ENDED - 1, SNAP_A, SNAP_B);
        checkSnapsAB();
    }

    private void checkSnapsAB() {
        Request req = requestRepo.getRequests().get(REQUEST_ID);
        assertEquals(ENDED, req.getOldestLine());

        Span spanA = req.getSnap(SNAP_A);
        Span spanB = req.getSnap(SNAP_B);

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
