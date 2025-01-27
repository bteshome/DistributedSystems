package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.JsonSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Utils;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class OffsetState {
    private final String table;
    private final int partition;
    private final String nodeId;
    private final Map<String, LogPosition> replicaEndOffsets;
    private LogPosition committedOffset;
    private final ReentrantReadWriteLock lock;
    private final String replicaEndOffsetsSnapshotFile;
    private final String committedOffsetSnapshotFile;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        replicaEndOffsets = new ConcurrentHashMap<>();
        committedOffset = LogPosition.empty();
        lock = new ReentrantReadWriteLock(true);
        replicaEndOffsetsSnapshotFile = "%s/%s-%s/replicaEndOffsets.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        committedOffsetSnapshotFile = "%s/%s-%s/committedOffset.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        loadReplicaEndOffsetsFromSnapshot();
        loadCommittedOffsetFromSnapshot();
    }

    public Map<String, LogPosition> getReplicaEndOffsets() {
        Map<String, LogPosition> copy;
        try (AutoCloseableLock l = readLock()) {
            copy = new HashMap<>(replicaEndOffsets);
        }
        return copy;
    }

    public LogPosition getReplicaEndOffset(String replicaNodeId) {
        try (AutoCloseableLock l = readLock()) {
            return replicaEndOffsets.getOrDefault(replicaNodeId, LogPosition.empty());
        }
    }

    public void setReplicaEndOffset(String replicaNodeId, LogPosition offset) {
        try (AutoCloseableLock l = writeLock()) {
            replicaEndOffsets.put(replicaNodeId, offset);
        }
    }

    public void setReplicaEndOffsets(Map<String, LogPosition> offsets) {
        for (Map.Entry<String, LogPosition> offset : offsets.entrySet()) {
            if (!offset.getKey().equals(nodeId))
                replicaEndOffsets.put(offset.getKey(), offset.getValue());
        }
    }

    public LogPosition getCommittedOffset() {
        try (AutoCloseableLock l = readLock()) {
            return committedOffset;
        }
    }

    public void setCommittedOffset(LogPosition offset) {
        BufferedWriter writer = Utils.createWriter(committedOffsetSnapshotFile);
        try (writer; AutoCloseableLock l = writeLock()) {
            committedOffset = offset;
            // TODO - 1. change to Java serialization if desired 2. add checksum
            writer.write(JsonSerDe.serialize(offset));
        } catch (IOException e) {
            String errorMessage = "Error writing committed index for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void takeSnapshot() {
        if (replicaEndOffsets.isEmpty())
            return;

        BufferedWriter writer = Utils.createWriter(replicaEndOffsetsSnapshotFile);
        try (writer) {
            // TODO - 1. change to Java serialization 2. add checksum
            writer.write(JsonSerDe.serialize(replicaEndOffsets));
            log.debug("Took a snapshot of replica end offsets for table '{}' partition '{}'.", table, partition);
        } catch (IOException e) {
            String errorMessage = "Error taking a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void loadReplicaEndOffsetsFromSnapshot() {
        if (Files.notExists(Path.of(replicaEndOffsetsSnapshotFile)))
            return;

        BufferedReader reader = Utils.createReader(replicaEndOffsetsSnapshotFile);
        try (reader) {
            String line = reader.readLine();
            if (line != null) {
                replicaEndOffsets.putAll(JsonSerDe.deserialize(line, new TypeReference<HashMap<String, LogPosition>>(){}));
                log.debug("Loaded replica end offsets from snapshot for table '{}' partition '{}'.", table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void loadCommittedOffsetFromSnapshot() {
        if (Files.notExists(Path.of(committedOffsetSnapshotFile)))
            return;

        BufferedReader reader = Utils.createReader(committedOffsetSnapshotFile);
        try (reader) {
            String line = reader.readLine();
            if (line != null) {
                committedOffset = JsonSerDe.deserialize(line, LogPosition.class);
                log.debug("Loaded replica committed offset from snapshot for table '{}' partition '{}'.", table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of committed offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }
}
