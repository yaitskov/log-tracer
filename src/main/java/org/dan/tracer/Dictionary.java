package org.dan.tracer;

import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.map.hash.HashObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;

import java.nio.ByteBuffer;

public class Dictionary {
    private final HashObjIntMap<ByteBuffer> byName;
    private final HashIntObjMap<ByteBuffer> byId;

    private Dictionary(HashObjIntMap<ByteBuffer> byName,
            HashIntObjMap<ByteBuffer> byId) {
        this.byId = byId;
        this.byName = byName;
    }

    public static Dictionary create() {
        return new Dictionary(
                HashObjIntMaps.newMutableMap(),
                HashIntObjMaps.newMutableMap());
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
