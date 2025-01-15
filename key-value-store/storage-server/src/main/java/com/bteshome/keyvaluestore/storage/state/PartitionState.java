package com.bteshome.keyvaluestore.storage.state;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.StorageSettings;
import com.bteshome.keyvaluestore.storage.WAL;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

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
    private final Map<String, Long> endOffsets;
    private long commitedOffset = 0L;
    private final WAL wal;
    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock putLock;
    private final StorageSettings storageSettings;

    public PartitionState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        data = new ConcurrentHashMap<>();
        endOffsets = new ConcurrentHashMap<>();
        commitedOffset = 0L;
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        lock = new ReentrantReadWriteLock(true);
        putLock = new ReentrantReadWriteLock(true);
        this.storageSettings = storageSettings;
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    private AutoCloseableLock putLock() { return AutoCloseableLock.acquire(putLock.writeLock()); }

    public ResponseEntity<?> putItem(String key, String value) {
        log.debug("PUT '{}'='{}' to table '{}' partition '{}'.", key, value, table, partition);

        try (AutoCloseableLock l = putLock()) {
            long offset;

            try (AutoCloseableLock l2 = writeLock()) {
                offset = wal.appendLog("PUT", key, value);
                endOffsets.put(nodeId, offset);
            } catch (Exception e) {
                log.error("Error writing to WAL.", e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error writing to WAL.");
            }

            int minInSyncReplicas = MetadataCache.getInstance().getMinInSyncReplicas(table);
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos((Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_TIMEOUT_MS_KEY));
            long start = System.nanoTime();
            log.debug("Waiting for {} replicas to commit log entry at offset {}.", minInSyncReplicas, offset);

            while (System.nanoTime() - start < timeoutNanos) {
                long countOfReplicasThatCommited = getEndOffsetValues().stream().filter(o -> o >= offset).count();
                if (countOfReplicasThatCommited >= minInSyncReplicas) {
                    setCommitedOffset(offset);
                    applyItem(key, value);
                    return ResponseEntity.ok().build();
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException e) {
                    log.error("Error waiting for replicas to commit log entry.", e);
                }
            }

            return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timed out.");
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
        try (AutoCloseableLock l = writeLock()) {
            wal.appendLogs(logEntries);
            endOffsets.put(nodeId, wal.getEndIndex());
        } catch (Exception e) {
            log.error("Error writing to WAL.", e);
        }
    }

    // TODO - how many log entries max to send back per request?
    public ResponseEntity<?> getLogEntries(long afterOffset) {
        try (AutoCloseableLock l = readLock()) {
            WALFetchResponse response = new WALFetchResponse(
                    wal.readLog(afterOffset),
                    endOffsets,
                    commitedOffset);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error reading from WAL.", e);
            return ResponseEntity.ok(new WALFetchResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Error reading from WAL."));
        }
    }

    public Map<String, Long> getEndOffsets() {
        Map<String, Long> copy;

        try (AutoCloseableLock l = readLock()) {
            copy = new ConcurrentHashMap<>(endOffsets);
        }

        return copy;
    }

    public Collection<Long> getEndOffsetValues() {
        try (AutoCloseableLock l = readLock()) {
            return endOffsets.values();
        }
    }

    public long getEndOffset(String replica) {
        try (AutoCloseableLock l = readLock()) {
            return endOffsets.getOrDefault(replica, 0L);
        }
    }

    public void setEndOffset(String replica, long offset) {
        try (AutoCloseableLock l = writeLock()) {
            endOffsets.put(replica, offset);
        }
    }

    public void setEndOffsets(Map<String, Long> offsets) {
        try (AutoCloseableLock l = writeLock()) {
            endOffsets.putAll(offsets);
        }
    }

    public long getCommitedOffset() {
        try (AutoCloseableLock l = readLock()) {
            return commitedOffset;
        }
    }

    public void setCommitedOffset(long offset) {
        try (AutoCloseableLock l = writeLock()) {
            commitedOffset = offset;
        }
    }

    public void applyItem(String key, String value) {
        try (AutoCloseableLock l = writeLock()) {
            data.put(key, value);
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
