package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.entities.EntityType;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class MetadataCache {
    private Map<EntityType, Map<String, Object>> state = Map.of();
    private long lastFetchedVersion = 0L;
    private static MetadataCache instance;

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
}
