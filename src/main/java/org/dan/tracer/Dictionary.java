package org.dan.tracer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Dictionary {
    private final Map<ByteBuffer, Integer> byName;
    private final Map<Integer, ByteBuffer> byId;

    private Dictionary(Map<ByteBuffer, Integer> byName,
            Map<Integer, ByteBuffer> byId) {
        this.byId = byId;
        this.byName = byName;
    }

    public static Dictionary create() {
        return new Dictionary(new HashMap<>(), new HashMap<>());
    }

    public int add(ByteBuffer word) {
         int id = byName.getOrDefault(word, 0);
         if (id == 0) {
             byId.put(id = byId.size() + 1, word);
             byName.put(word, id);
         }
         return id;
    }

    public ByteBuffer getById(int serviceId) {
        return byId.get(serviceId);
    }
}
