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
    private final Map<String, Long> replicaEndOffsets;
    private int lastFetchLeaderTerm;
    private long fullyReplicatedOffset;
    private final ReentrantReadWriteLock lock;
    private final String replicaEndOffsetsFile;
    private final String fullyReplicatedOffsetFile;
    private final String leaderTermFile;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        replicaEndOffsets = new ConcurrentHashMap<>();
        lastFetchLeaderTerm = 0;
        fullyReplicatedOffset = 0L;
        lock = new ReentrantReadWriteLock(true);
        replicaEndOffsetsFile = "%s/%s-%s/replicaEndOffsets.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        fullyReplicatedOffsetFile = "%s/%s-%s/fullyReplicatedOffset.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        leaderTermFile = "%s/%s-%s/leaderTerm.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    public Map<String, Long> getReplicaEndOffsets() {
        Map<String, Long> copy;
        try (AutoCloseableLock l = readLock()) {
            copy = new HashMap<>(replicaEndOffsets);
        }
        return copy;
    }

    public long getFullyReplicatedOffset() {
        try (AutoCloseableLock l = readLock()) {
            return fullyReplicatedOffset;
        }
    }

    public void setReplicaEndOffset(String replicaNodeId, long offset) {
        BufferedWriter writer = createReplicaEndOffsetsWriter();
        try (writer; AutoCloseableLock l = writeLock()) {
            replicaEndOffsets.put(replicaNodeId, offset);
            writer.write(replicaEndOffsets.toString());
        } catch (IOException e) {
            String errorMessage = "Error writing replica end offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedWriter createReplicaEndOffsetsWriter() {
        try {
            return new BufferedWriter(new FileWriter(replicaEndOffsetsFile, false));
        } catch (IOException e) {
            String errorMessage = "Error creating replica end offsets writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedWriter createFullyReplicatedOffsetWriter() {
        try {
            return new BufferedWriter(new FileWriter(fullyReplicatedOffsetFile, false));
        } catch (IOException e) {
            String errorMessage = "Error creating fully replicated offset writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedWriter createLeaderTermWriter() {
        try {
            return new BufferedWriter(new FileWriter(leaderTermFile, false));
        } catch (IOException e) {
            String errorMessage = "Error creating leader term writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public Collection<Long> getReplicaEndOffsetValues() {
        try (AutoCloseableLock l = readLock()) {
            return replicaEndOffsets.values();
        }
    }

    public long getReplicaEndOffset(String replicaNodeId) {
        try (AutoCloseableLock l = readLock()) {
            return replicaEndOffsets.getOrDefault(replicaNodeId, 0L);
        }
    }

    public int getLastFetchLeaderTerm() {
        try (AutoCloseableLock l = readLock()) {
            return lastFetchLeaderTerm;
        }
    }

    public void setReplicaEndOffsets(Map<String, Long> offsets) {
        for (Map.Entry<String, Long> offset : offsets.entrySet()) {
            if (!offset.getKey().equals(nodeId)) {
                setReplicaEndOffset(offset.getKey(), offset.getValue());
            }
        }
    }

    public void setFullyReplicatedOffset(long offset) {
        BufferedWriter writer = createFullyReplicatedOffsetWriter();
        try (writer; AutoCloseableLock l = writeLock()) {
            fullyReplicatedOffset = offset;
            writer.write(Long.toString(offset));
        } catch (IOException e) {
            String errorMessage = "Error writing fully replicated offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void setLeaderTerm(int leaderTerm) {
        BufferedWriter writer = createLeaderTermWriter();
        try (writer; AutoCloseableLock l = writeLock()) {
            this.lastFetchLeaderTerm = leaderTerm;
            writer.write(Long.toString(leaderTerm));
        } catch (IOException e) {
            String errorMessage = "Error writing leader term for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }
}
