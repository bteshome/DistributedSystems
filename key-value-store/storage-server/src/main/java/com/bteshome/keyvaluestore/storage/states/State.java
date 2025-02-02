package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.core.StorageNodeMetadataRefresher;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Component
@Slf4j
public class State implements ApplicationListener<ContextClosedEvent> {
    @Getter
    private String nodeId;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, PartitionState>> partitionStates = new ConcurrentHashMap<>();
    private boolean lastHeartbeatSucceeded;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ScheduledExecutorService snapshotsScheduler;
    private ScheduledExecutorService walFlushingExecutor;
    private ExecutorService walFlushingWorker;
    private ScheduledExecutorService dataExpirationScheduler;
    @Autowired
    private StorageSettings storageSettings;
    @Autowired
    private ISRSynchronizer isrSynchronizer;
    @Autowired
    private StorageNodeMetadataRefresher storageNodeMetadataRefresher;
    private boolean closed = false;

    @PostConstruct
    public void postConstruct() {
        nodeId = Validator.notEmpty(storageSettings.getNode().getId());
    }

    public void initialize() {
        try (AutoCloseableLock l = writeLock()) {
            createStorageDirectoryIfNotExists();
            loadFromSnapshotsAndWALFiles();
            scheduleSnapshots();
            scheduleWALFlushing();
            scheduleDataExpirationMonitor();
        }
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        if (!closed) {
            try {
                log.info("Closing state.");
                for (ConcurrentHashMap<Integer, PartitionState> tableState : partitionStates.values()) {
                    for (PartitionState partitionState : tableState.values())
                        CompletableFuture.runAsync(partitionState::close);
                }
                if (snapshotsScheduler != null)
                    snapshotsScheduler.close();
                if (walFlushingExecutor != null)
                    walFlushingExecutor.close();
                if (walFlushingWorker != null)
                    walFlushingWorker.close();
                if (dataExpirationScheduler != null)
                    dataExpirationScheduler.close();
                closed = true;
            } catch (Exception e) {
                log.error("Error closing state.", e);
            }
        }
    }

    public PartitionState getPartitionState(String table, int partition) {
        if (!partitionStates.containsKey(table))
            return null;
        return partitionStates.get(table).getOrDefault(partition, null);
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

        PartitionState partitionState = getPartitionState(table, partition);

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

    public void tableCreated(Table table) {
        try (AutoCloseableLock l = writeLock()) {
            for (Partition partition : table.getPartitions().values()) {
                if (!partitionStates.containsKey(partition.getTableName()))
                    partitionStates.put(partition.getTableName(), new ConcurrentHashMap<>());
                if (!partitionStates.get(partition.getTableName()).containsKey(partition.getId())) {
                    partitionStates.get(partition.getTableName()).put(partition.getId(), new PartitionState(
                            partition.getTableName(),
                            partition.getId(),
                            storageSettings,
                            isrSynchronizer));
                }
            }
        }
    }

    public void newLeaderElected(NewLeaderElectedRequest request) {
        PartitionState partitionState = getPartitionState(request.getTableName(), request.getPartitionId());
        partitionState.newLeaderElected(request);
        storageNodeMetadataRefresher.fetch();
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
                    if (MetadataCache.getInstance().tableExists(table)) {
                        int partition = Integer.parseInt(parts[1]);
                        if (!partitionStates.containsKey(table))
                            partitionStates.put(table, new ConcurrentHashMap<>());
                        if (!partitionStates.get(table).containsKey(partition)) {
                            partitionStates.get(table).put(partition, new PartitionState(table,
                                    partition,
                                    storageSettings,
                                    isrSynchronizer));
                        }
                    }
                }
            });
        } catch (IOException e) {
            String errorMessage = "Error loading snapshots and WAL files.";
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void scheduleSnapshots() {
        try {
            long interval = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.SNAPSHOT_INTERVAL_MS_KEY);
            snapshotsScheduler = Executors.newSingleThreadScheduledExecutor();
            snapshotsScheduler.scheduleAtFixedRate(this::takeSnapshot, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled data snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling data snapshots.", e);
        }
    }

    private void scheduleWALFlushing() {
        try {
            // TODO - use a more explicit configurable value
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_INTERVAL_MS_KEY);
            walFlushingExecutor = Executors.newSingleThreadScheduledExecutor();
            walFlushingWorker = Executors.newFixedThreadPool(2);
            walFlushingExecutor.scheduleAtFixedRate(this::flushWAL, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled WAL flushing. The interval is '%s' ms.".formatted(interval));
        } catch (Exception e) {
            log.error("Error scheduling WAL flushing.", e);
        }
    }

    private void scheduleDataExpirationMonitor() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.EXPIRATION_MONITOR_INTERVAL_MS_KEY);
            dataExpirationScheduler = Executors.newSingleThreadScheduledExecutor();
            dataExpirationScheduler.scheduleAtFixedRate(this::checkForExpiredDataItems, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled item expiration monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling item expiration monitor.", e);
        }
    }

    private void takeSnapshot() {
        log.info("Taking snapshots of data.");
        long start = System.nanoTime();

        try {
            for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
                for (PartitionState partitionState : tableState.values())
                    partitionState.takeDataSnapshot();
            }
        } finally {
            long end = System.nanoTime();
            log.info("Finished taking snapshots of data. Took {} ms.", (end - start) / 1000000);
        }
    }

    private void flushWAL() {
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values())
                CompletableFuture.runAsync(partitionState.getWal()::flush, walFlushingWorker);
        }
    }

    private void checkForExpiredDataItems() {
        log.debug("Item expiration monitor about to check if any items have expired.");

        try {
            List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(nodeId);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                String table = ownedPartition.first();
                int partition = ownedPartition.second();
                PartitionState partitionState = getPartitionState(table, partition);

                if (partitionState == null)
                    continue;

                partitionState.deleteExpiredItems();
            }
        } catch (Exception e) {
            log.error("Error checking item expiration.", e);
        }
    }
}
