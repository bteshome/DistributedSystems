package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.entities.*;
import com.bteshome.keyvaluestore.common.entities.Replica;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MetadataCache {
    private Map<EntityType, Map<String, Object>> state = Map.of();
    private Map<String, Map<Integer, Boolean>> partitionsWithPausedFetch = new ConcurrentHashMap<>();
    private String heartbeatEndpoint;
    private static final String CURRENT = "current";
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    @Getter
    private final static MetadataCache instance = new MetadataCache();

    public void pauseFetch(String table, int partition) {
        if (!partitionsWithPausedFetch.containsKey(table))
            partitionsWithPausedFetch.put(table, new ConcurrentHashMap<>());
        partitionsWithPausedFetch.get(table).put(partition, true);
        log.info("Paused fetch for table '{}' partition '{}'.", table, partition);
    }

    public void resumeFetch(String table, int partition) {
        partitionsWithPausedFetch.get(table).remove(partition);
        log.info("Resumed fetch for table '{}' partition '{}'.", table, partition);
    }

    public boolean isFetchPaused(String table, int partition) {
        if (!partitionsWithPausedFetch.containsKey(table))
            return false;
        return partitionsWithPausedFetch.get(table).containsKey(partition);
    }

    public long getLastFetchedVersion() {
        try (AutoCloseableLock l = readLock()) {
            if (state.containsKey(EntityType.VERSION)) {
                return (Long)state.get(EntityType.VERSION).getOrDefault(CURRENT, 0L);
            }
            return 0L;
        }
    }

    public Object getConfiguration(String key) {
        try (AutoCloseableLock l = readLock()) {
            return state.get(EntityType.CONFIGURATION).get(key);
        }
    }

    public Map<String, Object> getConfigurations() {
        try (AutoCloseableLock l = readLock()) {
            return state.get(EntityType.CONFIGURATION);
        }
    }

    public String getHeartbeatEndpoint() {
        try (AutoCloseableLock l = readLock()) {
            return heartbeatEndpoint;
        }
    }

    public void setHeartbeatEndpoint(String heartbeatEndpoint) {
        try (AutoCloseableLock l = writeLock()) {
            this.heartbeatEndpoint = heartbeatEndpoint;
        }
    }

    public void setState(Map<EntityType, Map<String, Object>> state) {
        try (AutoCloseableLock l = writeLock()) {
            this.state = new HashMap<>(state);
        }
    }

    public String getLeaderNodeId(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            return table.getPartitions().get(partition).getLeader();
        }
    }

    public String getLeaderEndpoint(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            String leaderNodeId = table.getPartitions().get(partition).getLeader();
            if (leaderNodeId == null) {
                return null;
            }
            StorageNode leaderNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(leaderNodeId);
            return "%s:%s".formatted(leaderNode.getHost(), leaderNode.getPort());
        }
    }

    public int getLeaderTerm(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            return table.getPartitions().get(partition).getLeaderTerm();
        }
    }

    public List<String> getReplicaEndpoints(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            return table.getPartitions()
                    .get(partition)
                    .getReplicas()
                    .stream()
                    .map(nodeId -> {
                        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
                        return "%s:%s".formatted(node.getHost(), node.getPort());
                    })
                    .toList();
        }
    }

    public List<String> getReplicaNodeIds(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            return table.getPartitions()
                    .get(partition)
                    .getReplicas()
                    .stream()
                    .toList();
        }
    }

    public Set<String> getInSyncReplicas(String tableName, int partition) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).get(tableName);
            return new HashSet<>(table.getPartitions()
                    .get(partition)
                    .getInSyncReplicas());
        }
    }

    public List<Tuple<String, Integer>> getOwnedPartitions(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
            return node.getReplicaAssignmentSet()
                    .stream()
                    .filter(ReplicaAssignment::isLeader)
                    .map(a -> new Tuple<>(a.getTableName(), a.getPartitionIid()))
                    .toList();
        }
    }

    public List<Replica> getFollowedReplicas(String nodeId) {
        try (AutoCloseableLock l = readLock()) {
            StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
            return node.getReplicaAssignmentSet()
                    .stream()
                    .filter(ReplicaAssignment::isFollower)
                    .map(a -> new Replica(nodeId, a.getTableName(), a.getPartitionIid()))
                    .toList();
        }
    }

    public boolean tableExists(String tableName) {
        try (AutoCloseableLock l = readLock()) {
            return state.get(EntityType.TABLE).containsKey(tableName);
        }
    }

    public int getNumPartitions(String tableName) {
        try (AutoCloseableLock l = readLock()) {
            return ((Table)state.get(EntityType.TABLE).get(tableName)).getPartitions().size();
        }
    }

    public int getMinInSyncReplicas(String tableName) {
        try (AutoCloseableLock l = writeLock()) {
            return ((Table)state.get(EntityType.TABLE).get(tableName)).getMinInSyncReplicas();
        }
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }
}
