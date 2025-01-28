package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Validator;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.core.StorageNodeMetadataRefresher;
import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    @Autowired
    ISRSynchronizer isrSynchronizer;

    @Autowired
    StorageNodeMetadataRefresher storageNodeMetadataRefresher;

    @PostConstruct
    public void postConstruct() {
        nodeId = Validator.notEmpty(storageSettings.getNode().getId());
        partitionStates = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
    }

    public void initialize() {
        createStorageDirectoryIfNotExists();
        loadFromSnapshotsAndWALFiles();
        scheduleSnapshots();
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

    public void scheduleSnapshots() {
        try {
            long interval = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.SNAPSHOT_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::takeSnapshot, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled data and replica end offset snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling data and replica end offset snapshots.", e);
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
            partitionStates.get(table).put(partition, new PartitionState(table,
                                                                         partition,
                                                                         storageSettings,
                                                                         isrSynchronizer));
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

        return partitionState.getLogEntries(lastFetchOffset, maxNumRecords, replicaId);
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

    public void newLeaderElected(NewLeaderElectedRequest request) {
        MetadataCache.getInstance().pauseFetch(request.getTableName(), request.getPartitionId());
        PartitionState partitionState = getPartitionState(request.getTableName(), request.getPartitionId(), true);

        if (storageSettings.getNode().getId().equals(request.getNewLeaderId())) {
            log.info("This node elected as the new leader for table '{}' partition '{}'. Now performing offset synchronization and truncation.",
                    request.getTableName(),
                    request.getPartitionId());
            Set<String> isrEndpoints = MetadataCache.getInstance().getISREndpoints(request.getTableName(), request.getPartitionId());
            WALGetReplicaEndOffsetRequest walGetReplicaEndOffsetRequest = new WALGetReplicaEndOffsetRequest(request.getTableName(), request.getPartitionId());
            LogPosition thisReplicaEndOffset = partitionState.getOffsetState().getReplicaEndOffset(nodeId);
            LogPosition earliestISREndOffset = thisReplicaEndOffset;

            for (String isrEndpoint : isrEndpoints) {
                WALGetReplicaEndOffsetResponse response = RestClient.builder()
                        .build()
                        .post()
                        .uri("http://%s/api/wal/get-end-offset/".formatted(isrEndpoint))
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(walGetReplicaEndOffsetRequest)
                        .retrieve()
                        .toEntity(WALGetReplicaEndOffsetResponse.class)
                        .getBody();

                if (response.getEndOffset().isLessThan(earliestISREndOffset))
                    earliestISREndOffset = response.getEndOffset();
            }

            if (earliestISREndOffset.isLessThan(thisReplicaEndOffset)) {
                log.info("Detected uncommitted offsets from the previous leader. Truncating WAL to offset {}.", earliestISREndOffset);
                partitionState.truncateLogsTo(earliestISREndOffset);
            }

            partitionState.getOffsetState().setPreviousLeaderEndOffset(earliestISREndOffset);
        } else {
            partitionState.getOffsetState().clearPreviousLeaderEndOffset();
        }

        storageNodeMetadataRefresher.fetchAsync().thenRun(() ->
                MetadataCache.getInstance().resumeFetch(request.getTableName(), request.getPartitionId()));
    }

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
                        partitionStates.get(table).put(partition, new PartitionState(table,
                                                                                     partition,
                                                                                     storageSettings,
                                                                                     isrSynchronizer));
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
        log.info("Taking snapshots of data and replica end offsets.");
        long start = System.nanoTime();
        try {
            for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
                for (PartitionState partitionState : tableState.values()) {
                    partitionState.takeDataSnapshot();
                }
            }
        } finally {
            long end = System.nanoTime();
            log.info("Finished taking snapshots of data and replica end offsets. Took {} ms.", (end - start) / 1000000);
        }
    }
}
