package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class UnmanagedState {
    public static Map<String, Long> heartbeats = new HashMap<>();
    public static Map<EntityType, Map<String, Object>> state;
    private static final String CURRENT = "current";

    static {
        state = new HashMap<>();
        state.put(EntityType.TABLE, new HashMap<>());
        state.put(EntityType.STORAGE_NODE, new HashMap<>());
        state.put(EntityType.CONFIGURATION, new HashMap<>());
        state.put(EntityType.VERSION, new HashMap<>());
        state.get(EntityType.VERSION).put(CURRENT, 0L);
    }

    public static long getVersion() {
        return (Long)state.get(EntityType.VERSION).get(CURRENT);
    }

    public static void clear() {
        state.get(EntityType.TABLE).clear();
        state.get(EntityType.STORAGE_NODE).clear();
        state.get(EntityType.CONFIGURATION).clear();
        state.get(EntityType.VERSION).put(CURRENT, 0L);
        heartbeats.clear();
    }
}
