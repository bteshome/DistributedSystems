package com.bteshome.keyvaluestore.storage.states;

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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
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
    private final WAL wal;
    @Getter
    private final OffsetState offsetState;
    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock putLock;

    public PartitionState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        data = new ConcurrentHashMap<>();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        offsetState = new OffsetState(table, partition, storageSettings);
        lock = new ReentrantReadWriteLock(true);
        putLock = new ReentrantReadWriteLock(true);
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    public ResponseEntity<?> putItem(String key, String value) {
        log.debug("PUT '{}'='{}' to table '{}' partition '{}'.", key, value, table, partition);

        try (AutoCloseableLock l = writeLock()) {
            long offset;

            try {
                offset = wal.appendLog("PUT", key, value);
            } catch (Exception e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error writing to WAL.");
            }

            try {
                offsetState.setEndOffset(nodeId, offset);
            } catch (StorageServerException e) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
            }

            int minInSyncReplicas = MetadataCache.getInstance().getMinInSyncReplicas(table);
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos((Long) MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_TIMEOUT_MS_KEY));
            long start = System.nanoTime();
            log.debug("Waiting for {} replicas to commit log entry at offset {}.", minInSyncReplicas, offset);

            while (System.nanoTime() - start < timeoutNanos) {
                long countOfReplicasThatCommited = offsetState.getEndOffsetValues().stream().filter(o -> o >= offset).count();
                if (countOfReplicasThatCommited >= minInSyncReplicas) {
                    try {
                        offsetState.setCommitedOffset(offset);
                    } catch (StorageServerException e) {
                        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
                    }
                    data.put(key, value);
                    return ResponseEntity.ok().build();
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorMessage);
                }
            }

            String errorMessage = "Request timed out.";
            log.error(errorMessage);
            return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body(errorMessage);
        }
    }

    public ResponseEntity<?> getItem(String key) {
        try (AutoCloseableLock l = readLock()) {
            if (!data.containsKey(key)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            return ResponseEntity.ok(data.get(key));
        }
    }

    public void appendLogEntries(List<String> logEntries) {
        if (logEntries.isEmpty()) { return; }
        wal.appendLogs(logEntries);
        offsetState.setEndOffset(nodeId, wal.getEndIndex());
    }

    // TODO - how many log entries max to send back per request?
    public ResponseEntity<?> getLogEntries(long afterOffset) {
        try {
            List<String> entries = wal.readLog(afterOffset);
            Map<String, Long> endOffsets = offsetState.getEndOffsets();
            long commitedOffset = offsetState.getCommitedOffset();

            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatus(HttpStatus.OK)
                    .entries(entries)
                    .replicaEndOffsets(endOffsets)
                    .commitedOffset(commitedOffset)
                    .build());
        } catch (Exception e) {
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                    .errorMessage(e.getMessage())
                    .build());
        }
    }

    public void applyLogEntries(List<String> entries) {
        try (AutoCloseableLock l = writeLock()) {
            for (String entry : entries) {
                String[] parts = entry.split(" ");
                String key = parts[2];
                String value = parts[3];
                data.put(key, value);
            }
        }
    }

    @Override
    public void close() {
        if (wal != null) {
            wal.close();
        }
    }
}
