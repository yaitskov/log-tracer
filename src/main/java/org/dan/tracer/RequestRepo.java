package org.dan.tracer;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Map;

public class RequestRepo {
    private static final Logger logger = LoggerFactory.getLogger(RequestRepo.class);

    private final Dictionary serviceDictionary;
    private final WritableByteChannel outputCh;
    private final ByteBuffer outputBuf;
    private final HashLongObjMap<Request> requests = HashLongObjMaps.newMutableMap();

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
        Span callerSpan = request.getSnap(callerSnapId);
        if (callerSpan == null) {
            request.addSnap(callerSpan = new Span(callerSnapId));
        }
        Span span = request.getSnap(snapId);
        if (span == null) {
            request.addSnap(span = new Span(snapId));
        }
        span.setServiceId(serviceId);
        span.setStarted(started);
        span.setEnded(ended);
        callerSpan.addChild(span);
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
        if (logger.isInfoEnabled()) {
            logger.info("Auto end at {} removed requests {} and {} of them were complete",
                    LogLineParser.timeToString(oldestTime),
                    originalSize - requests.size(),
                    complete);
        }
    }

    Map<Long, Request> getRequests() {
        return requests;
    }
}
