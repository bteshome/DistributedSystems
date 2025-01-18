package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.*;
import com.bteshome.keyvaluestore.storage.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
@Slf4j
public class State {
    @Getter
    private String nodeId;
    private Map<String, Map<Integer, PartitionState>> partitionStates;
    private boolean lastHeartbeatSucceeded;
    private ReentrantReadWriteLock lock;

    @Autowired
    StorageSettings storageSettings;

    @PostConstruct
    public void init() {
        nodeId = Validator.notEmpty(storageSettings.getNode().getId());
        partitionStates = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
    }

    @PreDestroy
    public void destroy() {
        try {
            for (Map<Integer, PartitionState> p : partitionStates.values()) {
                for (PartitionState ps : p.values()) {
                    ps.close();
                }
            }
        } catch (Exception e) {
            log.error("Error closing WAL files.", e);
        }
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    private Map<Integer, PartitionState> ensureTableStateExists(String table) {
        try (AutoCloseableLock l = readLock()) {
            if (partitionStates.containsKey(table)) {
                return partitionStates.get(table);
            }
        }
        try (AutoCloseableLock l = writeLock()) {
            if (!partitionStates.containsKey(table)) {
                partitionStates.put(table, new ConcurrentHashMap<>());
            }
            return partitionStates.get(table);
        }
    }

    private PartitionState ensurePartitionStateExists(Map<Integer, PartitionState> tableState, String table, int partition) {
        try (AutoCloseableLock l = readLock()) {
            if (tableState.containsKey(partition)) {
                return tableState.get(partition);
            }
        }
        try (AutoCloseableLock l = writeLock()) {
            if (!tableState.containsKey(partition)) {
                try {
                    tableState.put(partition, new PartitionState(table, partition, storageSettings));
                } catch (Exception e) {
                    log.error("Error creating PartitionState.", e);
                }
            }
            return tableState.get(partition);
        }
    }

    public ResponseEntity<ItemPutResponse> putItem(String table, int partition, String key, String value) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatus(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }
        Map<Integer, PartitionState> tableState = ensureTableStateExists(table);
        PartitionState partitionState = ensurePartitionStateExists(tableState, table, partition);
        return partitionState.putItem(key, value);
    }

    public ResponseEntity<ItemGetResponse> getItem(String table, int partition, String key) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatus(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatus(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatus(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getItem(key);
    }

    public ResponseEntity<ItemListResponse> listItems(String table, int partition, int limit) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatus(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatus(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatus(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.listItems(limit);
    }

    public ResponseEntity<ItemCountResponse> countItems(String table, int partition) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemCountResponse.builder()
                    .httpStatus(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        if (!partitionStates.containsKey(table)) {
            return ResponseEntity.ok(ItemCountResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .build());
        }
        if (!partitionStates.get(table).containsKey(partition)) {
            return ResponseEntity.ok(ItemCountResponse.builder()
                    .httpStatus(HttpStatus.NOT_FOUND.value())
                    .build());
        }
        partitionState = partitionStates.get(table).get(partition);

        return ResponseEntity.ok(ItemCountResponse.builder()
                .httpStatus(HttpStatus.OK.value())
                .count(partitionState.countItems())
                .build());
    }

    public ResponseEntity<?> acknowledgeFetch(String table, int partition, long endIndex, String replicaId) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String errorMessage = "Not the leader for table '%s' partition '%s'.".formatted(table, partition);
            return ResponseEntity.status(HttpStatus.MOVED_PERMANENTLY).body(errorMessage);
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return ResponseEntity.notFound().build();
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.notFound().build();
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        partitionState.getOffsetState().setEndOffset(replicaId, endIndex);

        // TODO - change to trace()
        log.debug("Persisted a fetch acknowledge from replica '{}' for table '{}' partition '{}' end offset '{}'.",
                replicaId,
                table,
                partition,
                endIndex);

        return ResponseEntity.ok().build();
    }

    public ResponseEntity<?> fetch(String table, int partition, long afterIndex) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String errorMessage = "Not the leader for table '%s' partition '%s'.".formatted(table, partition);
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatus(HttpStatus.MOVED_PERMANENTLY)
                    .errorMessage(errorMessage)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table) || !partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatus(HttpStatus.NOT_FOUND)
                        .build());
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getLogEntries(afterIndex);
    }

    public void appendLogEntries(
            String table,
            int partition,
            List<String> logEntries,
            Map<String, Long> endOffsets,
            long commitedOffset) {
        Map<Integer, PartitionState> tableState = ensureTableStateExists(table);
        PartitionState partitionState = ensurePartitionStateExists(tableState, table, partition);
        partitionState.appendLogEntries(logEntries);
        partitionState.getOffsetState().setEndOffsets(endOffsets);
        partitionState.getOffsetState().setCommitedOffset(commitedOffset);
    }

    public void applyLogEntries(String table, int partition, List<String> entries) {
        Map<Integer, PartitionState> tableState = ensureTableStateExists(table);
        PartitionState partitionState = ensurePartitionStateExists(tableState, table, partition);
        partitionState.applyLogEntries(entries);
    }

    public boolean getLastHeartbeatSucceeded() {
        try (AutoCloseableLock l = readLock()) {
            return this.lastHeartbeatSucceeded;
        }
    }

    public void setLastHeartbeatSucceeded(boolean lastHeartbeatSucceeded) {
        try (AutoCloseableLock l = writeLock()) {
            this.lastHeartbeatSucceeded = lastHeartbeatSucceeded;
        }
    }

    public Map<String, Long> getEndOffsets(String table, int partition) {
        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return Map.of();
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return Map.of();
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getOffsetState().getEndOffsets();
    }

    public long getEndOffset(String table, int partition, String replica) {
        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return 0L;
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return 0L;
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getOffsetState().getEndOffset(replica);
    }

    public long getCommitedOffset(String table, int partition) {
        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return 0L;
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return 0L;
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getOffsetState().getCommitedOffset();
    }
}
