package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.LogPosition;
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

    @PostConstruct
    public void postConstruct() {
        nodeId = Validator.notEmpty(storageSettings.getNode().getId());
        partitionStates = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
    }

    public void initialize() {
        createStorageDirectoryIfNotExists();
        loadFromSnapshotsAndWALFiles();
        scheduleReplicaEndOffsetSnapshots();
    }

    @PreDestroy
    public void destroy() {
        try {
            if (executor != null)
                executor.close();
            for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
                for (PartitionState partitionState : tableState.values()) {
                    partitionState.close();
                }
            }
        } catch (Exception e) {
            log.error("Error closing state.", e);
        }
    }

    public void scheduleReplicaEndOffsetSnapshots() {
        try {
            long interval = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_END_OFFSETS_SNAPSHOT_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::takeSnapshot, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica end index snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica end index snapshots.", e);
        }
    }

    public PartitionState getPartitionState(String table,
                                            int partition,
                                            boolean createIfNotExists) {
        if (!partitionStates.containsKey(table)) {
            if (!createIfNotExists)
                return null;
            partitionStates.put(table, new ConcurrentHashMap<>());
        }
        if (!partitionStates.get(table).containsKey(partition)) {
            if (!createIfNotExists)
                return null;
            partitionStates.get(table).put(partition, new PartitionState(table, partition, storageSettings));
        }
        return partitionStates.get(table).get(partition);
    }

    public ResponseEntity<WALFetchResponse> fetch(String table,
                                                  int partition,
                                                  LogPosition lastFetchOffset,
                                                  int maxNumRecords,
                                                  String replicaId) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String errorMessage = "Not the leader for table '%s' partition '%s'.".formatted(table, partition);
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .errorMessage(errorMessage)
                    .build());
        }

        PartitionState partitionState = getPartitionState(table, partition, false);

        if (partitionState == null) {
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build());
        }

        partitionState.getOffsetState().setReplicaEndOffset(replicaId, lastFetchOffset);

        return partitionState.getLogEntries(lastFetchOffset, maxNumRecords);
    }

    public void appendLogEntries(String table,
                                 int partition,
                                 List<String> logEntries,
                                 Map<String, LogPosition> endOffsets,
                                 LogPosition commitedOffset) {
        PartitionState partitionState = getPartitionState(table, partition, true);
        partitionState.appendLogEntries(logEntries);
        partitionState.getOffsetState().setReplicaEndOffsets(endOffsets);
        partitionState.getOffsetState().setCommittedOffset(commitedOffset);
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

    // TODO
    /*public int getLastFetchedLeaderTerm(String table, int partition, String replica) {
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
    }*/

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
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

    private void takeSnapshot() {
        log.debug("Taking a snapshot of replica end offsets.");
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values()) {
                partitionState.getOffsetState().takeSnapshot();
            }
        }
    }
}
