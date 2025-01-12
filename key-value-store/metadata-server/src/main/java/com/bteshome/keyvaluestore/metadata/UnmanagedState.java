package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.StorageNodeStatus;
import lombok.Getter;
import lombok.Setter;
import org.apache.ratis.util.AutoCloseableLock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class UnmanagedState {
    private boolean leader = false;
    private Map<String, Long> heartbeats = new ConcurrentHashMap<>();
    private Map<String, UnmanagedState.StorageNode> storageNodes = new ConcurrentHashMap<>();
    private Map<String, Object> configuration = new ConcurrentHashMap<>();
    private Long version = 0L;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    @Getter
    private static final UnmanagedState instance = new UnmanagedState();

    @Getter
    @Setter
    public static class StorageNode {
        private String id;
        private StorageNodeStatus status;
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    public boolean isLeader() {
        try (AutoCloseableLock l = readLock()) {
            return leader;
        }
    }

    public void setLeader() {
        try (AutoCloseableLock l = writeLock()) {
            leader = true;
        }
    }

    public Long getHeartbeat(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            return heartbeats.getOrDefault(nodeId, null);
        }
    }

    public void setHeartbeat(String nodeId, long timestamp) {
        try (AutoCloseableLock l = writeLock()) {
            heartbeats.put(nodeId, timestamp);
        }
    }

    public long getVersion() {
        try (AutoCloseableLock l = readLock()) {
            return version;
        }
    }

    public void setVersion(long version) {
        try (AutoCloseableLock l = writeLock()) {
            this.version = version;
        }
    }

    public Object getConfiguration(String key) {
        try (AutoCloseableLock l = readLock()) {
            return configuration.get(key);
        }
    }

    public void setConfiguration(Map<String, Object> configuration) {
        try (AutoCloseableLock l = writeLock()) {
            this.configuration.putAll(configuration);
        }
    }

    public Set<String> getStorageNodeIds() {
        try (AutoCloseableLock l = readLock()) {
            return storageNodes.values().stream().map(StorageNode::getId).collect(Collectors.toSet());
        }
    }

    public StorageNodeStatus getStorageNodeStatus(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            return storageNodes.get(nodeId).status;
        }
    }

    public void setStorageNodeStatus(String nodeId, StorageNodeStatus status) {
        try (AutoCloseableLock l = writeLock()) {
            storageNodes.get(nodeId).setStatus(status);
        }
    }

    public void setStorageNodes(Collection<com.bteshome.keyvaluestore.common.entities.StorageNode> storageNodes) {
        try (AutoCloseableLock l = writeLock()) {
            storageNodes.stream().map(node -> {
                UnmanagedState.StorageNode storageNode = new UnmanagedState.StorageNode();
                storageNode.setId(node.getId());
                storageNode.setStatus(node.getStatus());
                return storageNode;
            }).forEach(node -> this.storageNodes.put(node.getId(), node));
        }
    }

    public void addStorageNode(com.bteshome.keyvaluestore.common.entities.StorageNode storageNode) {
        try (AutoCloseableLock l = writeLock()) {
            UnmanagedState.StorageNode node = new UnmanagedState.StorageNode();
            node.setId(storageNode.getId());
            node.setStatus(storageNode.getStatus());
            this.storageNodes.put(node.getId(), node);
        }
    }

    public void removeStorageNode(String storageNodeId) {
        try (AutoCloseableLock l = writeLock()) {
            this.storageNodes.remove(storageNodeId);
        }
    }

    public void clear() {
        try (AutoCloseableLock l = writeLock()) {
            heartbeats.clear();
            storageNodes.clear();
            configuration.clear();
            version = 0L;
            leader = false;
        }
    }
}
