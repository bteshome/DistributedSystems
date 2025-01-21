package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.responses.ItemGetResponse;
import com.bteshome.keyvaluestore.client.responses.ItemListResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ReentrantReadWriteLock lock;
    private final StorageSettings storageSettings;

    public PartitionState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        lock = new ReentrantReadWriteLock(true);
        data = new ConcurrentHashMap<>();
        this.storageSettings = storageSettings;
        createPartitionDirectoryIfNotExists();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        loadFromWALFile();
        offsetState = new OffsetState(table, partition, storageSettings);
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
        List<String> logEntriesFromFile = wal.readLog(0, Integer.MAX_VALUE);
        if (!logEntriesFromFile.isEmpty()) {
            int endIndex = Integer.parseInt(logEntriesFromFile.getLast().split(" ")[0]);
            wal.setEndIndex(endIndex);
            applyLogEntries(logEntriesFromFile);
            log.debug("Loaded {} log entries from WAL file for table '{}' partition '{}'.", logEntriesFromFile.size(), table, partition);
        }
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    public ResponseEntity<ItemPutResponse> putItem(String key, String value) {
        log.debug("Received PUT '{}'='{}' to table '{}' partition '{}'.", key, value, table, partition);

        try (AutoCloseableLock l = writeLock()) {
            int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
            long offset;

            try {
                offset = wal.appendLog(leaderTerm, "PUT", key, value);
            } catch (Exception e) {
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }

            try {
                offsetState.setReplicaEndOffset(nodeId, offset);
            } catch (StorageServerException e) {
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }

            int minInSyncReplicas = MetadataCache.getInstance().getMinInSyncReplicas(table);
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos((Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_TIMEOUT_MS_KEY));
            long start = System.nanoTime();
            log.debug("Waiting for {} replicas to acknowledge log entry at offset {}.", minInSyncReplicas, offset);

            while (System.nanoTime() - start < timeoutNanos) {
                long countOfReplicasThatCommited = offsetState.getReplicaEndOffsetValues().stream().filter(o -> o >= offset).count();
                if (countOfReplicasThatCommited >= minInSyncReplicas) {
                    try {
                        offsetState.setFullyReplicatedOffset(offset);
                    } catch (StorageServerException e) {
                        return ResponseEntity.ok(ItemPutResponse.builder()
                                .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                                .errorMessage(e.getMessage())
                                .build());
                    }
                    data.put(key, value);
                    log.debug("Successfully commited PUT '{}'='{}' to table '{}' partition '{}'.", key, value, table, partition);
                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.OK.value())
                            .build());
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
                }
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
        try (AutoCloseableLock l = readLock()) {
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
        try (AutoCloseableLock l = readLock()) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(data.entrySet().stream().limit(Math.min(limit, 100)).toList())
                    .build());
        }
    }

    public void appendLogEntries(List<String> logEntries) {
        if (logEntries.isEmpty()) { return; }
        wal.appendLogs(logEntries);
        offsetState.setReplicaEndOffset(nodeId, wal.getEndIndex());
    }

    public ResponseEntity<WALFetchResponse> getLogEntries(long lastFetchedEndOffset, int lastFetchedLeaderTerm, int maxNumRecords) {
        try {
            // TODO
            /*int currentLeaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
            if (lastFetchedLeaderTerm < currentLeaderTerm) {
                long endIndexForLastFetchedLeaderTerm = wal.getEndIndexForLeaderTerm(lastFetchedLeaderTerm);
                if (lastFetchedEndOffset > endIndexForLastFetchedLeaderTerm) {

                }
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                        .leaderEndpoint(MetadataCache.getInstance().getLeaderEndpoint(table, partition))
                        .build());
            }*/

            List<String> entries = wal.readLog(lastFetchedEndOffset, maxNumRecords);
            Map<String, Long> endOffsets = offsetState.getReplicaEndOffsets();
            long commitedOffset = offsetState.getFullyReplicatedOffset();

            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .entries(entries)
                    .replicaEndOffsets(endOffsets)
                    .leaderTerm(MetadataCache.getInstance().getLeaderTerm(table, partition))
                    .commitedOffset(commitedOffset)
                    .build());
        } catch (Exception e) {
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build());
        }
    }

    public void applyLogEntries(List<String> entries) {
        try (AutoCloseableLock l = writeLock()) {
            for (String logEntry : entries) {
                WALEntry walEntry = WALEntry.fromString(logEntry);
                switch (walEntry.getOperation()) {
                    case "PUT" -> data.put(walEntry.getKey(), walEntry.getValue());
                    case "DELETE" -> data.remove(walEntry.getKey());
                }
            }
        }
    }

    public int countItems() {
        return data.size();
    }

    @Override
    public void close() {
        if (wal != null) {
            wal.close();
        }
    }
}
