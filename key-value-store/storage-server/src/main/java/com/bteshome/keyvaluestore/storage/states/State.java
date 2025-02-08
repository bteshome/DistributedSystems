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
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Component
@Slf4j
public class State implements ApplicationListener<ContextClosedEvent> {
    @Getter
    private String nodeId;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, PartitionState>> partitionStates = new ConcurrentHashMap<>();
    @Autowired
    private StorageSettings storageSettings;
    @Autowired
    private ISRSynchronizer isrSynchronizer;
    @Autowired
    WebClient webClient;
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
        try {
            partitionStates.put(table.getName(), new ConcurrentHashMap<>());
            for (Partition partition : table.getPartitions().values()) {
                partitionStates.get(partition.getTableName()).put(partition.getId(), new PartitionState(
                        partition.getTableName(),
                        partition.getId(),
                        storageSettings,
                        isrSynchronizer,
                        webClient));
            }
        } catch (Exception e) {
            String errorMessage = "Error handling TABLE CREATED notification for table '%s'.".formatted(table.getName());
            log.error(errorMessage, e);
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
                                    isrSynchronizer,
                                    webClient));
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

            Flux.interval(Duration.ofMillis(interval))
                    .doOnNext(this::checkReplicaStatus)
                    .subscribe();

            log.info("Scheduled replication monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replication monitor.", e);
        }
    }

    public void scheduleWalFetcher() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_INTERVAL_MS_KEY);

            Flux.interval(Duration.ofMillis(interval))
                    .doOnNext(this::fetchWal)
                    .subscribe();

            log.info("Scheduled replica WAL fetcher. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling replica WAL fetcher.", e);
        }
    }

    private void scheduleDataSnapshots() {
        try {
            long interval = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.DATA_SNAPSHOT_INTERVAL_MS_KEY);

            Flux.interval(Duration.ofMillis(interval))
                    .doOnNext(this::takeDataSnapshots)
                    .subscribe();

            log.info("Scheduled data snapshots. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling data snapshots.", e);
        }
    }

    private void scheduleDataExpirationMonitor() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.EXPIRATION_MONITOR_INTERVAL_MS_KEY);

            Flux.interval(Duration.ofMillis(interval))
                    .doOnNext(this::checkForExpiredDataItems)
                    .subscribe();

            log.info("Scheduled item expiration monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling item expiration monitor.", e);
        }
    }

    private void checkReplicaStatus(long tick) {
        Flux.fromIterable(partitionStates.values())
                .flatMap(tableState -> Flux.fromIterable(tableState.values()))
                .doOnNext(partitionState -> {
                    Mono.fromRunnable(partitionState::checkReplicaStatus)
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe();
                })
                .subscribe();
    }

    private void fetchWal(long tick) {
        Flux.fromIterable(partitionStates.values())
                .flatMap(tableState -> Flux.fromIterable(tableState.values()))
                .doOnNext(partitionState -> {
                    Mono.fromRunnable(partitionState::fetchWal)
                            .subscribeOn(Schedulers.boundedElastic())
                            .subscribe();
                })
                .subscribe();
    }

    private void takeDataSnapshots(long tick) {
        Flux.fromIterable(partitionStates.values())
                .flatMap(tableState -> Flux.fromIterable(tableState.values()))
                .doOnNext(PartitionState::takeDataSnapshot)
                .subscribe();
    }

    private void checkForExpiredDataItems(long tick) {
        Flux.fromIterable(partitionStates.values())
                .flatMap(tableState -> Flux.fromIterable(tableState.values()))
                .doOnNext(PartitionState::deleteExpiredItems)
                .subscribe();
    }
}
