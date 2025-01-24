package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.JsonSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private final Map<String, LogPosition> replicaEndOffsets;
    private LogPosition committedOffset;
    private final ReentrantReadWriteLock lock;
    private final String replicaEndOffsetsFile;
    private final String committedOffsetFile;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        replicaEndOffsets = new ConcurrentHashMap<>();
        committedOffset = LogPosition.empty();
        lock = new ReentrantReadWriteLock(true);
        replicaEndOffsetsFile = "%s/%s-%s/replicaEndOffsets.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        committedOffsetFile = "%s/%s-%s/committedOffset.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
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

    public Collection<LogPosition> getReplicaEndOffsetValues() {
        try (AutoCloseableLock l = readLock()) {
            return replicaEndOffsets.values();
        }
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
        BufferedWriter writer = createWriter(committedOffsetFile);
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

        BufferedWriter writer = createWriter(replicaEndOffsetsFile);
        try (writer) {
            // TODO - 1. change to Java serialization if desired 2. add checksum
            writer.write(JsonSerDe.serialize(replicaEndOffsets));
            log.debug("Took a snapshot of replica end offsets for table '{}' partition '{}'.", table, partition);
        } catch (IOException e) {
            String errorMessage = "Error taking a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void loadReplicaEndOffsetsFromSnapshot() {
        if (Files.notExists(Path.of(replicaEndOffsetsFile)))
            return;

        BufferedReader reader = createReader(replicaEndOffsetsFile);
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
        if (Files.notExists(Path.of(committedOffsetFile)))
            return;

        BufferedReader reader = createReader(committedOffsetFile);
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

    private BufferedWriter createWriter(String fileName) {
        try {
            return new BufferedWriter(new FileWriter(fileName, false));
        } catch (IOException e) {
            String errorMessage = "Error creating replica offsets snapshot writer for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private BufferedReader createReader(String fileName) {
        try {
            return new BufferedReader(new FileReader(fileName));
        } catch (IOException e) {
            String errorMessage = "Error creating replica offsets snapshot reader for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }
}
