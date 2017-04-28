package org.dan.tracer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class RequestRepoTest {
    @Test
    public void autoEndSkip() {
        RequestRepo requestRepo = new RequestRepo(null, null, null);
        Request request = new Request(1);
        request.updateLastTimeStamp(100L);
        requestRepo.getRequests().put(1L, request);
        requestRepo.autoEnd(101L, 5);
        assertEquals(1, requestRepo.getRequests().size());
    }

    @Test
    public void autoEndCollects() {
        RequestRepo requestRepo = new RequestRepo(null, null, null);
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
}
