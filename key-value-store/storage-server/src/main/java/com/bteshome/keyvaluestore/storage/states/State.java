package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Component
@Slf4j
public class State {
    @Getter
    private String nodeId;
    private Map<String, Map<Integer, PartitionState>> partitionStates;
    private boolean lastHeartbeatSucceeded;
    private ReentrantReadWriteLock lock;
    private ScheduledExecutorService executor = null;

    @Autowired
    StorageSettings storageSettings;

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
                tableState.put(partition, new PartitionState(table, partition, storageSettings));
            }
            return tableState.get(partition);
        }
    }

    private void createStorageDirectoryIfNotExists() {
        Path storageDirectory = Path.of(storageSettings.getNode().getStorageDir());
        try {
            if (Files.notExists(storageDirectory)) {
                Files.createDirectory(storageDirectory);
                log.info("Storage directory '%s' created.".formatted(storageDirectory));
            }
        } catch (Exception e) {
            String errorMessage = "Error creating storage directory '%s'.".formatted(storageDirectory);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void loadFromSnapshotsAndWALFiles() {
        Path storageDirectory = Path.of(storageSettings.getNode().getStorageDir());

        try (Stream<Path> partitionDirectories = Files.list(storageDirectory)) {
            partitionDirectories.forEach(partitionDirectory -> {
                if (Files.isDirectory(partitionDirectory)) {
                    String[] parts = partitionDirectory.getFileName().toString().split("-");
                    String table = parts[0];
                    int partition = Integer.parseInt(parts[1]);
                    if (!partitionStates.containsKey(table)) {
                        partitionStates.put(table, new ConcurrentHashMap<>());
                    }
                    if (!partitionStates.get(table).containsKey(partition)) {
                        partitionStates.get(table).put(partition, new PartitionState(table, partition, storageSettings));
                    }
                }
            });
        } catch (IOException e) {
            String errorMessage = "Error loading snapshots and WAL files.";
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void scheduleReplicaEndOffsetSnapshots() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_END_OFFSETS_SNAPSHOT_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::takeSnapshot, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica end offset snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica end offset snapshots.", e);
        }
    }

    private void takeSnapshot() {
        log.debug("Taking a snapshot of replica end offsets.");
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values()) {
                partitionState.getOffsetState().takeSnapshot();
            }
        }
    }

    public void initialize() {
        nodeId = Validator.notEmpty(storageSettings.getNode().getId());
        partitionStates = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
        createStorageDirectoryIfNotExists();
        loadFromSnapshotsAndWALFiles();
        scheduleReplicaEndOffsetSnapshots();
    }

    @PreDestroy
    public void destroy() {
        try {
            if (executor != null) {
                executor.close();
            }
            for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
                for (PartitionState partitionState : tableState.values()) {
                    partitionState.close();
                }
            }
        } catch (Exception e) {
            log.error("Error closing state.", e);
        }
    }

    public ResponseEntity<ItemPutResponse> putItem(String table, int partition, String key, String value) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
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
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
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
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.listItems(limit);
    }

    public ResponseEntity<ItemCountAndOffsetsResponse> countItems(String table, int partition) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        PartitionState partitionState;

        if (!partitionStates.containsKey(table)) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }
        if (!partitionStates.get(table).containsKey(partition)) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }
        partitionState = partitionStates.get(table).get(partition);

        return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .count(partitionState.countItems())
                .commitedOffset(partitionState.getOffsetState().getFullyReplicatedOffset())
                .replicaEndOffsets(partitionState.getOffsetState().getReplicaEndOffsets())
                .build());
    }

    public ResponseEntity<WALFetchResponse> fetch(String table, int partition, long lastFetchedEndOffset, int lastFetchedLeaderTerm, int maxNumRecords, String replicaId) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String errorMessage = "Not the leader for table '%s' partition '%s'.".formatted(table, partition);
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table) || !partitionStates.get(table).containsKey(partition)) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        partitionState.getOffsetState().setReplicaEndOffset(replicaId, lastFetchedEndOffset);

        return partitionState.getLogEntries(lastFetchedEndOffset, lastFetchedLeaderTerm, maxNumRecords);
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
        partitionState.getOffsetState().setReplicaEndOffsets(endOffsets);
        partitionState.getOffsetState().setFullyReplicatedOffset(commitedOffset);
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

    public Map<String, Long> getReplicaEndOffsets(String table, int partition) {
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

        return partitionState.getOffsetState().getReplicaEndOffsets();
    }

    public long getReplicaEndOffset(String table, int partition, String replica) {
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

        return partitionState.getOffsetState().getReplicaEndOffset(replica);
    }

    public long getFullyReplicatedOffset(String table, int partition) {
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

        return partitionState.getOffsetState().getFullyReplicatedOffset();
    }

    public long getReplicaOffset(String table, int partition) {
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

        return partitionState.getOffsetState().getFullyReplicatedOffset();
    }

    public int getLastFetchedLeaderTerm(String table, int partition, String replica) {
        PartitionState partitionState;

        try (AutoCloseableLock l = readLock()) {
            if (!partitionStates.containsKey(table)) {
                return 0;
            }
            if (!partitionStates.get(table).containsKey(partition)) {
                return 0;
            }
            partitionState = partitionStates.get(table).get(partition);
        }

        return partitionState.getWal().getEndLeaderTerm();
    }
}
