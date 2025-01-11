package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class MetadataCache {
    private Map<EntityType, Map<String, Object>> state = Map.of();
    private String heartbeatEndpoint;
    private static MetadataCache instance;
    private static final String CURRENT = "current";

    private synchronized static void createInstance() {
        if (instance == null) {
            instance = new MetadataCache();
        }
    }

    public static MetadataCache getInstance() {
        if (instance == null) {
            createInstance();
        }
        return instance;
    }

    public long getLastFetchedVersion() {
        if (state.containsKey(EntityType.VERSION)) {
            return (Long)state.get(EntityType.VERSION).getOrDefault(CURRENT, 0L);
        }
        return 0L;
    }
}
