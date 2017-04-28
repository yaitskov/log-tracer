package org.dan.tracer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.dan.tracer.LogLineParser.NULL_SPAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.sun.org.apache.regexp.internal.RE;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;

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
    public void firstFirstLine() {
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED + 1, ENDED - 1, SNAP_A, SNAP_B);
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED, ENDED, NULL_SPAN, SNAP_A);
        checkSnapsAB();
    }

    private void checkSnapsAB() {
        Request req = requestRepo.getRequests().get(REQUEST_ID);
        assertEquals(ENDED, req.getOldestLine());

        Snap snapA = req.getSnap(SNAP_A);
        Snap snapB = req.getSnap(SNAP_B);

        assertEquals(snapB.getParent(), snapA);
        assertNull(snapA.getParent().getParent());
        assertEquals(NULL_SPAN, snapA.getParent().getId());
        assertEquals(snapB.getChildren(), emptyList());
        assertEquals(snapA.getChildren(), singletonList(snapB));
        assertEquals(snapA.getStarted(), STARTED);
        assertEquals(snapA.getEnded(), ENDED);
        assertEquals(snapA.getServiceId(), SERVICE_ID);
        assertEquals(snapA.getId(), SNAP_A);

        assertEquals(snapB.getId(), SNAP_B);
        assertEquals(snapB.getStarted(), STARTED + 1);
        assertEquals(snapB.getEnded(), ENDED - 1);
        assertEquals(snapB.getServiceId(), SERVICE_ID);
    }


    @Test
    public void lineOfNestedTraceFirst() {
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED + 1, ENDED - 1, SNAP_A, SNAP_B);
        requestRepo.line(SERVICE_ID, REQUEST_ID, STARTED, ENDED, NULL_SPAN, SNAP_A);
        checkSnapsAB();
    }
}
