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
    private static boolean leader = false;
    private static Map<String, Long> heartbeats = new ConcurrentHashMap<>();
    private static Map<String, UnmanagedState.StorageNode> storageNodes = new ConcurrentHashMap<>();
    private static Map<String, Object> configuration = new ConcurrentHashMap<>();
    private static Long version = 0L;
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @Getter
    @Setter
    public static class StorageNode {
        private String id;
        private StorageNodeStatus status;
    }

    private static AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private static AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    public static boolean isLeader() {
        try (AutoCloseableLock l = readLock()) {
            return leader;
        }
    }

    public static void setLeader() {
        try (AutoCloseableLock l = writeLock()) {
            leader = true;
        }
    }

    public static Long getHeartbeat(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            return heartbeats.getOrDefault(nodeId, null);
        }
    }

    public static void setHeartbeat(String nodeId, long timestamp) {
        try (AutoCloseableLock l = writeLock()) {
            heartbeats.put(nodeId, timestamp);
        }
    }

    public static long getVersion() {
        try (AutoCloseableLock l = readLock()) {
            return version;
        }
    }

    public static void setVersion(long version) {
        try (AutoCloseableLock l = writeLock()) {
            UnmanagedState.version = version;
        }
    }

    public static Object getConfiguration(String key) {
        try (AutoCloseableLock l = readLock()) {
            return configuration.get(key);
        }
    }

    public static void setConfiguration(Map<String, Object> configuration) {
        try (AutoCloseableLock l = writeLock()) {
            UnmanagedState.configuration.putAll(configuration);
        }
    }

    public static Set<String> getStorageNodeIds() {
        try (AutoCloseableLock l = readLock()) {
            return storageNodes.values().stream().map(StorageNode::getId).collect(Collectors.toSet());
        }
    }

    public static StorageNodeStatus getStorageNodeStatus(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            return storageNodes.get(nodeId).status;
        }
    }

    public static void setStorageNodeStatus(String nodeId, StorageNodeStatus status) {
        try (AutoCloseableLock l = writeLock()) {
            storageNodes.get(nodeId).setStatus(status);
        }
    }

    public static void setStorageNodes(Collection<com.bteshome.keyvaluestore.common.entities.StorageNode> storageNodes) {
        try (AutoCloseableLock l = writeLock()) {
            storageNodes.stream().map(node -> {
                UnmanagedState.StorageNode storageNode = new UnmanagedState.StorageNode();
                storageNode.setId(node.getId());
                storageNode.setStatus(node.getStatus());
                return storageNode;
            }).forEach(node -> UnmanagedState.storageNodes.put(node.getId(), node));
        }
    }

    public static void addStorageNode(com.bteshome.keyvaluestore.common.entities.StorageNode storageNode) {
        try (AutoCloseableLock l = writeLock()) {
            UnmanagedState.StorageNode node = new UnmanagedState.StorageNode();
            node.setId(storageNode.getId());
            node.setStatus(storageNode.getStatus());
            UnmanagedState.storageNodes.put(node.getId(), node);
        }
    }

    public static void removeStorageNode(String storageNodeId) {
        try (AutoCloseableLock l = writeLock()) {
            UnmanagedState.storageNodes.remove(storageNodeId);
        }
    }

    public static void clear() {
        try (AutoCloseableLock l = writeLock()) {
            heartbeats.clear();
            storageNodes.clear();
            configuration.clear();
            version = 0L;
            leader = false;
        }
    }
}
