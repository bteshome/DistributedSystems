package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.JsonSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Utils;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
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
    private LogPosition previousLeaderEndOffset;
    private final ReentrantReadWriteLock lock;
    private final String replicaEndOffsetsSnapshotFile;
    private final String committedOffsetSnapshotFile;
    private final String previousLeaderEndOffsetFile;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        replicaEndOffsets = new ConcurrentHashMap<>();
        committedOffset = LogPosition.empty();
        previousLeaderEndOffset = LogPosition.empty();
        lock = new ReentrantReadWriteLock(true);
        replicaEndOffsetsSnapshotFile = "%s/%s-%s/replicaEndOffsets.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        committedOffsetSnapshotFile = "%s/%s-%s/committedOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        previousLeaderEndOffsetFile = "%s/%s-%s/previousLeaderEndOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        loadReplicaEndOffsetsFromSnapshot();
        loadCommittedOffsetFromSnapshot();
        loadPreviousLeaderEndOffset();
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
            // TODO - change to Java serialization once done testing
            writer.write(JsonSerDe.serialize(offset));
            ChecksumUtil.generateAndWrite(committedOffsetSnapshotFile);
            log.debug("Persisted committed offset '{}' for table '{}' partition '{}'.", committedOffset, table, partition);
        } catch (IOException e) {
            String errorMessage = "Error writing committed offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }


    public LogPosition getPreviousLeaderEndOffset() {
        try (AutoCloseableLock l = readLock()) {
            if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition)))
                throw new StorageServerException("Illegal access. Not the leader for table '%s' partition '%s'.".formatted(table, partition));
            return previousLeaderEndOffset;
        }
    }

    public void setPreviousLeaderEndOffset(LogPosition offset) {
        BufferedWriter writer = Utils.createWriter(previousLeaderEndOffsetFile);
        try (writer; AutoCloseableLock l = writeLock()) {
            previousLeaderEndOffset = offset;
            writer.write(JsonSerDe.serialize(offset));
            log.debug("Persisted previous leader end offset '{}' for table '{}' partition '{}'.", previousLeaderEndOffset, table, partition);
        } catch (IOException e) {
            String errorMessage = "Error writing previous leader end offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void clearPreviousLeaderEndOffset() {
        try (AutoCloseableLock l = writeLock()) {
            previousLeaderEndOffset = LogPosition.empty();
            Files.deleteIfExists(Path.of(previousLeaderEndOffsetFile));
            Files.deleteIfExists(Path.of(previousLeaderEndOffsetFile + ".md5"));
            log.debug("Deleted previous leader end offset file '{}' for table '{}' partition '{}'.", previousLeaderEndOffsetFile, table, partition);
        } catch (IOException e) {
            String errorMessage = "Error deleting previous leader end offset file for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void takeSnapshot() {
        if (replicaEndOffsets.isEmpty())
            return;

        BufferedWriter writer = Utils.createWriter(replicaEndOffsetsSnapshotFile);
        try (writer) {
            // TODO - 1. change to Java serialization once done testing
            writer.write(JsonSerDe.serialize(replicaEndOffsets));
            ChecksumUtil.generateAndWrite(replicaEndOffsetsSnapshotFile);
            log.debug("Took a snapshot of replica end offsets for table '{}' partition '{}'.", table, partition);
        } catch (IOException e) {
            String errorMessage = "Error taking a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void loadReplicaEndOffsetsFromSnapshot() {
        if (Files.notExists(Path.of(replicaEndOffsetsSnapshotFile)))
            return;

        ChecksumUtil.readAndVerify(replicaEndOffsetsSnapshotFile);

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

    private void loadCommittedOffsetFromSnapshot() {
        if (Files.notExists(Path.of(committedOffsetSnapshotFile)))
            return;

        ChecksumUtil.readAndVerify(committedOffsetSnapshotFile);

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

    private void loadPreviousLeaderEndOffset() {
        if (Files.notExists(Path.of(previousLeaderEndOffsetFile)))
            return;

        ChecksumUtil.readAndVerify(previousLeaderEndOffsetFile);

        BufferedReader reader = Utils.createReader(previousLeaderEndOffsetFile);
        try (reader) {
            String line = reader.readLine();
            if (line != null) {
                previousLeaderEndOffset = JsonSerDe.deserialize(line, LogPosition.class);
                log.debug("Loaded previous leader end offset from snapshot for table '{}' partition '{}'.", table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of previous leader end offset for table '%s' partition '%s'.".formatted(table, partition);
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
