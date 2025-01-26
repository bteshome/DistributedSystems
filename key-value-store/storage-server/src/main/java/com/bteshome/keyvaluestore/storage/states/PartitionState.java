package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class PartitionState implements AutoCloseable {
    private final String table;
    private final int partition;
    private final String nodeId;
    private final Map<String, String> data;
    @Getter
    private final WAL wal;
    @Getter
    private final OffsetState offsetState;
    private final ReentrantReadWriteLock dataLock;
    private final ReentrantReadWriteLock operationLock;
    private final StorageSettings storageSettings;
    private final ISRSynchronizer isrSynchronizer;

    public PartitionState(String table,
                          int partition,
                          StorageSettings storageSettings,
                          ISRSynchronizer isrSynchronizer) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        dataLock = new ReentrantReadWriteLock(true);
        operationLock = new ReentrantReadWriteLock(true);
        data = new ConcurrentHashMap<>();
        this.storageSettings = storageSettings;
        this.isrSynchronizer = isrSynchronizer;
        createPartitionDirectoryIfNotExists();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        loadFromWALFile();
        offsetState = new OffsetState(table, partition, storageSettings);
    }

    public ResponseEntity<ItemPutResponse> putItems(List<Item> items) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        log.debug("Received PUT request for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

        try (AutoCloseableLock l = writeOperationLock()) {
            int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
            LogPosition offset;

            try {
                long index = wal.appendItems(leaderTerm, "PUT", items);
                offset = LogPosition.of(leaderTerm, index);
                offsetState.setReplicaEndOffset(nodeId, offset);
            } catch (Exception e) {
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }

            long timeoutMs = (Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            long start = System.nanoTime();
            log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);
            Set<Replica> newISRs = null;
            Set<Replica> laggingCurrentISRs = null;

            while (System.nanoTime() - start < timeoutNanos) {
                Tuple3<Boolean, Set<Replica>, Set<Replica>> acknowledgementCheckResult = isFullyAcknowledged(offset);
                boolean canBeCommitted = acknowledgementCheckResult.first();
                newISRs = acknowledgementCheckResult.second();
                laggingCurrentISRs = acknowledgementCheckResult.third();

                if (canBeCommitted) {
                    try {
                        offsetState.setCommittedOffset(offset);
                    } catch (StorageServerException e) {
                        return ResponseEntity.ok(ItemPutResponse.builder()
                                .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                                .errorMessage(e.getMessage())
                                .build());
                    }

                    try (AutoCloseableLock l2 = writeDataLock()) {
                        for (Item item : items)
                            data.put(item.getKey(), item.getValue());
                    }

                    if (newISRs != null && !newISRs.isEmpty()) {
                        final Set<Replica> newISRsFinal = newISRs;
                        CompletableFuture.runAsync(() -> isrSynchronizer.addToInSyncReplicaLists(newISRsFinal));
                    }

                    log.debug("Successfully commited PUT '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.OK.value())
                            .build());
                }

                // TODO - replace the thread sleep with ExecutorService
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
                }
            }

            if (laggingCurrentISRs != null && !laggingCurrentISRs.isEmpty()) {
                final Set<Replica> laggingReplicasFinal = laggingCurrentISRs;
                CompletableFuture.runAsync(() -> isrSynchronizer.removeFromInSyncReplicaLists(laggingReplicasFinal));
            }

            String errorMessage = "Request timed out.";
            log.error(errorMessage);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.REQUEST_TIMEOUT.value())
                    .errorMessage(errorMessage)
                    .build());
        }
    }

    public ResponseEntity<ItemGetResponse> getItem(String key) {
        try (AutoCloseableLock l = readDataLock()) {
            if (!data.containsKey(key)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .value(data.get(key))
                    .build());
        }
    }

    public ResponseEntity<ItemListResponse> listItems(int limit) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        try (AutoCloseableLock l = readDataLock()) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(data.entrySet().stream().limit(Math.min(limit, 100)).toList())
                    .build());
        }
    }

    public void appendLogEntries(List<WALEntry> logEntries) {
        if (logEntries.isEmpty())
            return;

        wal.appendLogs(logEntries);
        offsetState.setReplicaEndOffset(nodeId, wal.getEndOffset());
    }

    public ResponseEntity<WALFetchResponse> getLogEntries(LogPosition lastFetchOffset, int maxNumRecords) {
        try {
            int currentLeaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);

            if (lastFetchOffset.leaderTerm() < currentLeaderTerm) {
                long endIndexForLastFetchLeaderTerm = wal.getEndIndexForLeaderTerm(lastFetchOffset.leaderTerm());
                if (lastFetchOffset.index() > endIndexForLastFetchLeaderTerm) {
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.CONFLICT.value())
                            .truncateToOffset(LogPosition.of(lastFetchOffset.leaderTerm(), endIndexForLastFetchLeaderTerm))
                            .build());
                }
            }

            List<WALEntry> entries = wal.readLogs(lastFetchOffset, maxNumRecords);
            Map<String, LogPosition> endOffsets = offsetState.getReplicaEndOffsets();
            LogPosition commitedOffset = offsetState.getCommittedOffset();

            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .entries(entries)
                    .replicaEndOffsets(endOffsets)
                    .commitedOffset(commitedOffset)
                    .build());
        } catch (Exception e) {
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build());
        }
    }

    public void reApplyLogEntries() {
        try (AutoCloseableLock l = writeDataLock()) {
            data.clear();
        }
        applyLogEntries(getWal().readLogs());
    }

    public void applyLogEntries(List<WALEntry> entries) {
        try (AutoCloseableLock l = writeDataLock()) {
            for (WALEntry walEntry : entries) {
                switch (walEntry.operation()) {
                    case "PUT" -> data.put(walEntry.key(), walEntry.value());
                    case "DELETE" -> data.remove(walEntry.key());
                }
            }
        }
    }

    public ResponseEntity<ItemCountAndOffsetsResponse> countItems() {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .count(data.size())
                .commitedOffset(offsetState.getCommittedOffset())
                .replicaEndOffsets(offsetState.getReplicaEndOffsets())
                .build());
    }

    @Override
    public void close() {
        if (wal != null) {
            wal.close();
        }
    }

    private Tuple3<Boolean, Set<Replica>, Set<Replica>> isFullyAcknowledged(LogPosition offset) {
        int minISRCount = MetadataCache.getInstance().getMinInSyncReplicas(table);
        Set<Replica> replicasThatAcknowledged = offsetState.getReplicaEndOffsets()
                .entrySet()
                .stream()
                .filter(replicaOffset -> replicaOffset.getValue().compareTo(offset) >= 0)
                .map(replica -> new Replica(replica.getKey(), table, partition))
                .collect(Collectors.toSet());
        Set<String> currentISRNodeIds = MetadataCache.getInstance().getInSyncReplicas(table, partition);

        if (replicasThatAcknowledged.size() < minISRCount) {
            Set<Replica> currentISRsThatDidNotAcknowledge = currentISRNodeIds
                    .stream()
                    .filter(nodeId -> replicasThatAcknowledged.stream()
                            .noneMatch(replica -> replica.getNodeId().equals(nodeId)))
                    .map(nodeId -> new Replica(nodeId, table, partition))
                    .collect(Collectors.toSet());
            return Tuple3.of(false, null, currentISRsThatDidNotAcknowledge);
        } else {
            Set<Replica> newISRs = replicasThatAcknowledged.stream()
                    .filter(replica -> !currentISRNodeIds.contains(replica.getNodeId()))
                    .collect(Collectors.toSet());
            return Tuple3.of(true, newISRs, null);
        }
    }

    private void createPartitionDirectoryIfNotExists() {
        Path partitionDir = Path.of("%s/%s-%s".formatted(storageSettings.getNode().getStorageDir(), table, partition));
        try {
            if (Files.notExists(partitionDir)) {
                Files.createDirectory(partitionDir);
                log.debug("Partition directory '%s' created.".formatted(partitionDir));
            }
        } catch (Exception e) {
            String errorMessage = "Error creating partition directory '%s'.".formatted(partitionDir);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void loadFromWALFile() {
        List<WALEntry> logEntriesFromFile = wal.loadFromFile();
        if (!logEntriesFromFile.isEmpty()) {
            applyLogEntries(logEntriesFromFile);
            log.debug("Loaded {} log entries from WAL file for table '{}' partition '{}'.", logEntriesFromFile.size(), table, partition);
        }
    }

    private AutoCloseableLock readDataLock() {
        return AutoCloseableLock.acquire(dataLock.readLock());
    }

    private AutoCloseableLock writeDataLock() {
        return AutoCloseableLock.acquire(dataLock.writeLock());
    }

    private AutoCloseableLock writeOperationLock() {
        return AutoCloseableLock.acquire(operationLock.writeLock());
    }
}
