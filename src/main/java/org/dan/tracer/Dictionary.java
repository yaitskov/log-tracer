package org.dan.tracer;

import static com.koloboke.collect.map.hash.HashIntObjMaps.newMutableMap;

import com.koloboke.collect.map.hash.HashIntObjMap;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Dictionary {
    private final AtomicInteger idAllocator;
    private final ConcurrentMap<ByteBuffer, Integer> byName;
    private final HashIntObjMap<ByteBuffer> byId;

    private Dictionary(AtomicInteger idAllocator,
            ConcurrentMap<ByteBuffer, Integer> byName,
            HashIntObjMap<ByteBuffer> byId) {
        this.idAllocator = idAllocator;
        this.byId = byId;
        this.byName = byName;
    }

    public static Dictionary create() {
        return new Dictionary(
                new AtomicInteger(),
                new ConcurrentHashMap<>(),
                newMutableMap());
    }

    public Dictionary fork() {
        return new Dictionary(idAllocator, byName, newMutableMap());
    }

    public int add(ByteBuffer word) {
        final int id = idAllocator.incrementAndGet();
        final Integer wasId = byName.putIfAbsent(word, id);
        if (wasId == null) {
            byId.put(id, word);
            return id;
        } else {
            byId.put((int) wasId, word);
            return wasId;
        }
    }

    public ByteBuffer getById(int serviceId) {
        return byId.get(serviceId);
    }
}
