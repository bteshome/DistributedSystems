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

import java.io.*;
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
    private final ConcurrentHashMap<String, String> data;
    @Getter
    private final WAL wal;
    @Getter
    private final OffsetState offsetState;
    private final ReentrantReadWriteLock dataLock;
    private final ReentrantReadWriteLock operationLock;
    private final StorageSettings storageSettings;
    private final ISRSynchronizer isrSynchronizer;
    private final String dataSnapshotFile;

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
        dataSnapshotFile = "%s/%s-%s/data.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        loadFromDataSnapshotAndWALFile();
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

            try (AutoCloseableLock l2 = writeDataLock()) {
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
                    try (AutoCloseableLock l2 = writeDataLock()) {
                        offsetState.setCommittedOffset(offset);
                        for (Item item : items)
                            data.put(item.getKey(), item.getValue());
                    } catch (StorageServerException e) {
                        return ResponseEntity.ok(ItemPutResponse.builder()
                                .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                                .errorMessage(e.getMessage())
                                .build());
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

    public void appendLogEntries(List<WALEntry> logEntries,
                                 Map<String, LogPosition> replicaEndOffsets,
                                 LogPosition commitedOffset) {
        try (AutoCloseableLock l = writeDataLock()) {
            LogPosition endOffset = wal.appendLogs(logEntries);
            offsetState.setReplicaEndOffset(nodeId, endOffset);
            offsetState.setReplicaEndOffsets(replicaEndOffsets);
            offsetState.setCommittedOffset(commitedOffset);

            for (WALEntry walEntry : logEntries) {
                switch (walEntry.operation()) {
                    case "PUT" -> data.put(walEntry.key(), walEntry.value());
                    case "DELETE" -> data.remove(walEntry.key());
                }
            }
        }
    }

    public void truncateLogsTo(LogPosition toOffset) {
        try (AutoCloseableLock l = writeDataLock()) {
            wal.truncateToBeforeInclusive(toOffset);
            offsetState.setReplicaEndOffset(nodeId, toOffset);
            // TODO - is this always correct?
            offsetState.setCommittedOffset(toOffset);
            data.clear();
            for (WALEntry walEntry : wal.readLogs()) {
                switch (walEntry.operation()) {
                    case "PUT" -> data.put(walEntry.key(), walEntry.value());
                    case "DELETE" -> data.remove(walEntry.key());
                }
            }
        }
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

    public void takeDataSnapshot() {
        BufferedWriter writer = Utils.createWriter(dataSnapshotFile);

        try (writer; AutoCloseableLock l = readDataLock()) {
            DataSnapshot dataSnapshot = new DataSnapshot();
            dataSnapshot.setLastCommittedOffset(offsetState.getCommittedOffset());
            dataSnapshot.setData(data);
            writer.write(JavaSerDe.serialize(dataSnapshot));
            wal.truncateToAfterExclusive(dataSnapshot.getLastCommittedOffset());
            offsetState.takeSnapshot();
            log.debug("Took a snapshot of data upto offset '{}' for table '{}' partition '{}'.",
                      dataSnapshot.getLastCommittedOffset(),
                      table,
                      partition);
        } catch (Exception e) {
            String errorMessage = "Error taking a snapshot of data for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
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

    private void loadFromDataSnapshotAndWALFile() {
        try (AutoCloseableLock l = writeDataLock()) {
            if (Files.exists(Path.of(dataSnapshotFile))) {
                BufferedReader reader = Utils.createReader(dataSnapshotFile);
                try (reader) {
                    String line = reader.readLine();
                    if (line != null) {
                        DataSnapshot dataSnapshot = JavaSerDe.deserialize(line);
                        data.putAll(dataSnapshot.getData());
                        log.debug("Loaded '{}' data items from a snapshot up to offset '{}' for table '{}' partition '{}' .",
                                  dataSnapshot.getData().size(),
                                  dataSnapshot.getLastCommittedOffset(),
                                  table,
                                  partition);
                    }
                } catch (IOException e) {
                    String errorMessage = "Error loading data from a snapshot for table '%s' partition '%s'.".formatted(table, partition);
                    log.error(errorMessage, e);
                    throw new StorageServerException(errorMessage, e);
                }
            }

            List<WALEntry> logEntriesFromFile = wal.loadFromFile();
            for (WALEntry walEntry : logEntriesFromFile) {
                switch (walEntry.operation()) {
                    case "PUT" -> data.put(walEntry.key(), walEntry.value());
                    case "DELETE" -> data.remove(walEntry.key());
                }
            }

            log.debug("Loaded '{}' log entries from WAL file for table '{}' partition '{}'.",
                      logEntriesFromFile.size(),
                      table,
                      partition);
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
