package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class OffsetState {
    private final String table;
    private final int partition;
    private final String nodeId;
    private final Map<String, Long> endOffsets;
    private long commitedOffset = 0L;
    private final ReentrantReadWriteLock lock;
    private final String endOffsetsFile;
    private final String commitedOffsetFile;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        endOffsets = new ConcurrentHashMap<>();
        commitedOffset = 0L;
        lock = new ReentrantReadWriteLock(true);
        endOffsetsFile = "%s/%s-%s/endOffsets.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        commitedOffsetFile = "%s/%s-%s/commitedOffset.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    public Map<String, Long> getEndOffsets() {
        Map<String, Long> copy;
        try (AutoCloseableLock l = readLock()) {
            copy = new HashMap<>(endOffsets);
        }
        return copy;
    }

    public long getCommitedOffset() {
        try (AutoCloseableLock l = readLock()) {
            return commitedOffset;
        }
    }

    public void setEndOffset(String replica, long offset) {
        BufferedWriter writer = createEndOffsetsWriter();
        try (writer; AutoCloseableLock l = writeLock()) {
            endOffsets.put(replica, offset);
            writer.write(endOffsets.toString());
        } catch (IOException e) {
            String errorMessage = "Error writing end offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedWriter createEndOffsetsWriter() {
        try {
            return new BufferedWriter(new FileWriter(endOffsetsFile, false));
        } catch (IOException e) {
            String errorMessage = "Error creating end offsets writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedWriter createCommitedOffsetWriter() {
        try {
            return new BufferedWriter(new FileWriter(commitedOffsetFile, false));
        } catch (IOException e) {
            String errorMessage = "Error creating commited offset writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
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

    public void setEndOffsets(Map<String, Long> endOffsets) {
        for (Map.Entry<String, Long> replicaOffset : endOffsets.entrySet()) {
            if (!replicaOffset.getKey().equals(nodeId)) {
                setEndOffset(replicaOffset.getKey(), replicaOffset.getValue());
            }
        }
    }

    public void setCommitedOffset(long offset) {
        BufferedWriter writer = createCommitedOffsetWriter();
        try (writer; AutoCloseableLock l = writeLock()) {
            commitedOffset = offset;
            writer.write(Long.toString(offset));
        } catch (IOException e) {
            String errorMessage = "Error writing commited offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }
}
