package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.common.Validator;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.ApplicationScope;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class ReplicationState {
    @Getter
    private String id;
    private Map<String, Map<Integer, Map<String, Long>>> offsets;
    @Getter
    @Setter
    private boolean lastHeartbeatSucceeded;
    private ReentrantReadWriteLock lock;
    @Autowired
    StorageSettings storageSettings;

    @PostConstruct
    public void init() {
        id = Validator.notEmpty(storageSettings.getNode().get("id"));
        offsets = new ConcurrentHashMap<>();
        lastHeartbeatSucceeded = false;
        lock = new ReentrantReadWriteLock(true);
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    public long getOffset(String table, int partition, String replica) {
        try (AutoCloseableLock l = readLock()) {
            if (!offsets.containsKey(table)) {
                return 0;
            }
            if (!offsets.get(table).containsKey(partition)) {
                return 0;
            }
            return offsets.get(table).get(partition).getOrDefault(replica, 0L);
        }
    }

    public void setOffset(String table, int partition, String replica, long offset) {
        try (AutoCloseableLock l = writeLock()) {
            if (!offsets.containsKey(table)) {
                offsets.put(table, new ConcurrentHashMap<>());
            }
            if (!offsets.get(table).containsKey(partition)) {
                offsets.get(table).put(partition, new ConcurrentHashMap<>());
            }
            offsets.get(table).get(partition).put(replica, offset);
        }
    }
}
