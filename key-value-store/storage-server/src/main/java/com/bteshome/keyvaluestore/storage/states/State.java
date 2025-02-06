package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.core.StorageNodeMetadataRefresher;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Component
@Slf4j
public class State implements ApplicationListener<ContextClosedEvent> {
    @Getter
    private String nodeId;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, PartitionState>> partitionStates = new ConcurrentHashMap<>();
    private ScheduledExecutorService replicationMonitorScheduler;
    private ScheduledExecutorService walFetcherScheduler;
    private ScheduledExecutorService snapshotsScheduler;
    private ScheduledExecutorService dataExpirationScheduler;
    private ScheduledExecutorService logTimestampsTrimmerScheduler;
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
        createStorageDirectoryIfNotExists();
        loadFromSnapshotsAndWALFiles();
        scheduleReplicationMonitor();
        scheduleWalFetcher();
        //scheduleDataSnapshots();
        scheduleDataExpirationMonitor();
        scheduleLogTimestampsTrimmer();
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        if (!closed) {
            try {
                log.info("Closing state.");
                for (ConcurrentHashMap<Integer, PartitionState> tableState : partitionStates.values()) {
                    for (PartitionState partitionState : tableState.values())
                        partitionState.close();
                }
                if (replicationMonitorScheduler != null)
                    replicationMonitorScheduler.close();
                if (walFetcherScheduler != null)
                    walFetcherScheduler.close();
                if (snapshotsScheduler != null)
                    snapshotsScheduler.close();
                if (dataExpirationScheduler != null)
                    dataExpirationScheduler.close();
                if (logTimestampsTrimmerScheduler != null)
                    logTimestampsTrimmerScheduler.close();
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

    public void tableCreated(Table table) {
        storageNodeMetadataRefresher.fetch();
        partitionStates.put(table.getName(), new ConcurrentHashMap<>());
        for (Partition partition : table.getPartitions().values()) {
            partitionStates.get(partition.getTableName()).put(partition.getId(), new PartitionState(
                        partition.getTableName(),
                        partition.getId(),
                        storageSettings,
                        isrSynchronizer));
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

    private void scheduleReplicationMonitor() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_MONITOR_INTERVAL_MS_KEY);
            int numThreads = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_MONITOR_THREAD_POOL_SIZE_KEY);
            replicationMonitorScheduler = Executors.newScheduledThreadPool(numThreads);
            replicationMonitorScheduler.scheduleWithFixedDelay(this::checkReplicaStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replication monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replication monitor.", e);
        }
    }

    public void scheduleWalFetcher() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_INTERVAL_MS_KEY);
            int numThreads = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_THREAD_POOL_SIZE_KEY);
            walFetcherScheduler = Executors.newScheduledThreadPool(numThreads);
            walFetcherScheduler.scheduleWithFixedDelay(this::fetchWal, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled replica WAL fetcher. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica WAL fetcher.", e);
        }
    }

    private void scheduleDataSnapshots() {
        try {
            long interval = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.DATA_SNAPSHOT_INTERVAL_MS_KEY);
            snapshotsScheduler = Executors.newSingleThreadScheduledExecutor();
            snapshotsScheduler.scheduleWithFixedDelay(this::takeDataSnapshots, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled data snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling data snapshots.", e);
        }
    }

    private void scheduleDataExpirationMonitor() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.EXPIRATION_MONITOR_INTERVAL_MS_KEY);
            dataExpirationScheduler = Executors.newSingleThreadScheduledExecutor();
            dataExpirationScheduler.scheduleWithFixedDelay(this::checkForExpiredDataItems, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled item expiration monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling item expiration monitor.", e);
        }
    }

    public void scheduleLogTimestampsTrimmer() {
        try {
            // TODO - should be configurable?
            long interval = Duration.ofMinutes(15).toMillis();
            logTimestampsTrimmerScheduler = Executors.newSingleThreadScheduledExecutor();
            logTimestampsTrimmerScheduler.scheduleWithFixedDelay(this::trimLogTimestamps, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled log timestamps trimmer. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling log timestamps trimmer.", e);
        }
    }

    private void checkReplicaStatus() {
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values()) {
                partitionState.checkReplicaStatus();
            }
        }
    }

    private void fetchWal() {
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values()) {
                partitionState.fetchWal();
            }
        }
    }

    private void takeDataSnapshots() {
        log.debug("Taking snapshots of data.");
        long start = System.nanoTime();

        try {
            for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
                for (PartitionState partitionState : tableState.values())
                    partitionState.takeDataSnapshot();
            }
        } finally {
            long end = System.nanoTime();
            log.debug("Finished taking snapshots of data. Took {} ms.", (end - start) / 1000000);
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

    private void trimLogTimestamps() {
        for (Map<Integer, PartitionState> tableState : partitionStates.values()) {
            for (PartitionState partitionState : tableState.values())
                partitionState.getOffsetState().trimLogTimestamps();
        }
    }
}
