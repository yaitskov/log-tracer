package org.dan.tracer;

import java.util.HashMap;
import java.util.Map;

public class Dictionary {
    private final Map<String, Integer> byName;
    private final Map<Integer, String> byId;

    private Dictionary(Map<String, Integer> byName,
            Map<Integer, String> byId) {
        this.byId = byId;
        this.byName = byName;
    }

    public static Dictionary create() {
        return new Dictionary(new HashMap<>(), new HashMap<>());
    }

    public int add(String word) {
         int id = byName.getOrDefault(word, 0);
         if (id == 0) {
             byId.put(id = byId.size() + 1, word);
             byName.put(word, id);
         }
         return id;
    }

    public String getById(int serviceId) {
        return byId.get(serviceId);
    }
}
