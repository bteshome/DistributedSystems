package com.bteshome.keyvaluestore.common;

import com.bteshome.keyvaluestore.common.entities.*;
import com.bteshome.keyvaluestore.common.entities.Replica;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class MetadataCache {
    private final Map<EntityType, Map<String, Object>> state;
    private String heartbeatEndpoint;
    private static final String CURRENT = "current";
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    @Getter
    private final static MetadataCache instance = new MetadataCache();

    public MetadataCache() {
        this.state = new ConcurrentHashMap<>();
        state.put(EntityType.TABLE, new ConcurrentHashMap<>());
        state.put(EntityType.STORAGE_NODE, new ConcurrentHashMap<>());
        state.put(EntityType.CONFIGURATION, new ConcurrentHashMap<>());
        state.put(EntityType.VERSION, new ConcurrentHashMap<>());
        state.get(EntityType.VERSION).put(CURRENT, 0L);
    }

    public long getLastFetchedVersion() {
        if (state.containsKey(EntityType.VERSION))
            return (Long)state.get(EntityType.VERSION).getOrDefault(CURRENT, 0L);
        return 0L;
    }

    public Object getConfiguration(String key) {
        return state.get(EntityType.CONFIGURATION).get(key);
    }

    public Map<String, Object> getConfigurations() {
        return new HashMap<>(state.get(EntityType.CONFIGURATION));
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
        this.state.get(EntityType.TABLE).putAll(state.get(EntityType.TABLE));
        this.state.get(EntityType.STORAGE_NODE).putAll(state.get(EntityType.STORAGE_NODE));
        this.state.get(EntityType.CONFIGURATION).putAll(state.get(EntityType.CONFIGURATION));
        this.state.get(EntityType.VERSION).putAll(state.get(EntityType.VERSION));
    }

    public String getState() {
        return JavaSerDe.serialize(state);
    }

    public Duration getTableTimeToLive(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return table.getTimeToLive();
    }

    public String getLeaderNodeId(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return table.getPartitions().get(partition).getLeader();
    }

    public String getLeaderEndpoint(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).getOrDefault(tableName, null);
        if (table == null)
            return null;
        String leaderNodeId = table.getPartitions().get(partition).getLeader();
        if (leaderNodeId == null)
            return null;
        StorageNode leaderNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(leaderNodeId);
        return "%s:%s".formatted(leaderNode.getHost(), leaderNode.getPort());
    }

    public int getLeaderTerm(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return table.getPartitions().get(partition).getLeaderTerm();
    }

    public String getEndpoint(String nodeId) {
        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
        return "%s:%s".formatted(node.getHost(), node.getPort());
    }

    public Set<String> getISREndpoints(String tableName, int partition, String excludedNodeId) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return table.getPartitions()
                .get(partition)
                .getInSyncReplicas()
                .stream()
                .filter(nodeId -> !nodeId.equals(excludedNodeId))
                .map(nodeId -> {
                    StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
                    return "%s:%s".formatted(node.getHost(), node.getPort());
                })
                .collect(Collectors.toSet());
    }

    public Set<String> getReplicaNodeIds(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return new HashSet<>(table.getPartitions()
                .get(partition)
                .getReplicas());
    }

    public Set<String> getInSyncReplicas(String tableName, int partition) {
        Table table = (Table)state.get(EntityType.TABLE).get(tableName);
        return new HashSet<>(table.getPartitions()
                .get(partition)
                .getInSyncReplicas());
    }

    public List<Tuple<String, Integer>> getOwnedPartitions(String nodeId) {
        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
        return node.getReplicaAssignmentSet()
                .stream()
                .filter(ReplicaAssignment::isLeader)
                .map(a -> new Tuple<>(a.getTableName(), a.getPartitionIid()))
                .toList();
    }

    public List<Replica> getFollowedReplicas(String nodeId) {
        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(nodeId);
        return node.getReplicaAssignmentSet()
                .stream()
                .filter(ReplicaAssignment::isFollower)
                .map(a -> new Replica(nodeId, a.getTableName(), a.getPartitionIid()))
                .toList();
    }

    public boolean tableExists(String tableName) {
        try (AutoCloseableLock l = readLock()) {
            return state.get(EntityType.TABLE).containsKey(tableName);
        }
    }

    public int getNumPartitions(String tableName) {
        try (AutoCloseableLock l = readLock()) {
            Table table = (Table)state.get(EntityType.TABLE).getOrDefault(tableName, null);
            if (table == null)
                throw new NotFoundException("Table '%s' not found.".formatted(tableName));
            return table.getPartitions().size();
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
