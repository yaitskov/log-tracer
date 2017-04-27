package org.dan.tracer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class RequestRepo {
    private static final Logger logger = LoggerFactory.getLogger(RequestRepo.class);

    private final Dictionary serviceDictionary;
    private final WritableByteChannel outputCh;
    private final ByteBuffer outputBuf;
    private final Map<Long, Request> requests = new HashMap<>();

    public RequestRepo(Dictionary serviceDictionary,
                       WritableByteChannel outputCh,
                       ByteBuffer outputBuf) {
        this.serviceDictionary = serviceDictionary;
        this.outputCh = outputCh;
        this.outputBuf = outputBuf;
    }

    public void line(int serviceId, long requestId, long started,
            long ended, long callerSnapId, long snapId) {
        Request request = requests.get(requestId);
        if (request == null) {
            request = new Request(requestId);
            requests.put(requestId, request);
        }
        request.updateLastTimeStamp(ended);
        Snap callerSnap = request.getSnap(callerSnapId);
        if (callerSnap == null) {
            request.addSnap(callerSnap = new Snap(callerSnapId));
        }
        Snap snap = request.getSnap(snapId);
        if (snap == null) {
            request.addSnap(snap = new Snap(snapId));
        }
        snap.setServiceId(serviceId);
        snap.setStarted(started);
        snap.setEnded(ended);
        callerSnap.addChild(snap);
    }

    public void autoEnd(final long oldestTime, final long autoEndMs) {
        // close and json all request with oldestTime - last time >= autoEndMs
        final Iterator<Map.Entry<Long, Request>> iter = requests.entrySet().iterator();
        int originalSize = requests.size();
        int complete = 0;
        while (iter.hasNext()) {
            Map.Entry<Long, Request> entry = iter.next();
            Request request = entry.getValue();
            if (oldestTime - request.getOldestLine() >= autoEndMs) {
                iter.remove();
                complete += request.writeAsJson(outputCh, outputBuf, serviceDictionary);
            }
        }
        logger.info("Auto end at {} removed requests {} and {} of them were complete",
                    oldestTime, originalSize - requests.size(), complete);
    }
}
