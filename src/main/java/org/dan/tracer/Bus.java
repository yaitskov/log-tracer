package org.dan.tracer;

import static java.lang.String.valueOf;
import static java.util.Optional.ofNullable;

import com.koloboke.collect.map.hash.HashLongObjMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Bus {
    private static final class BlockAssociation
            extends AwaitableVar<HashLongObjMap<Request>>{
        public BlockAssociation(String label) {
            super(label);
        }
    }

    private final ConcurrentMap<Integer, BlockAssociation> block2Requests
            = new ConcurrentHashMap<>();

    public HashLongObjMap<Request> peekTransitRequests(int blockId) {
        return get(blockId).peek();
    }

    public HashLongObjMap<Request> getTransitRequests(int blockId) throws InterruptedException {
        return get(blockId)
                .get();
    }

    private BlockAssociation get(int blockId) {
        BlockAssociation value = new BlockAssociation(valueOf(blockId));
        return ofNullable(block2Requests.putIfAbsent(blockId, value))
                .orElse(value);
    }

    public void putTransitRequests(int blockId, HashLongObjMap<Request> transitRequests) {
        get(blockId).set(transitRequests);
    }

    public static int getWorkerThreads() {
        return Runtime.getRuntime().availableProcessors();
    }
}
