package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.JsonSerDe;
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
    private final Map<String, Long> replicaEndOffsets;
    private long fullyReplicatedOffset;
    private final ReentrantReadWriteLock lock;
    private final String replicaEndOffsetsFile;
    StorageSettings storageSettings;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        replicaEndOffsets = new ConcurrentHashMap<>();
        fullyReplicatedOffset = 0L;
        lock = new ReentrantReadWriteLock(true);
        replicaEndOffsetsFile = "%s/%s-%s/replicaEndOffsets.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        this.storageSettings = storageSettings;
        loadFromSnapshot();
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

    public void setReplicaEndOffset(String replicaNodeId, long offset) {
        replicaEndOffsets.put(replicaNodeId, offset);
    }

    public void setReplicaEndOffsets(Map<String, Long> offsets) {
        for (Map.Entry<String, Long> offset : offsets.entrySet()) {
            if (!offset.getKey().equals(nodeId)) {
                replicaEndOffsets.put(offset.getKey(), offset.getValue());
            }
        }
    }

    // TODO - write to a kafka topic instead.
    public void setFullyReplicatedOffset(long offset) {
        String fullyReplicatedOffsetFile = "%s/%s-%s/fullyReplicatedOffset.log".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        BufferedWriter writer = createWriter(fullyReplicatedOffsetFile);
        try (writer; AutoCloseableLock l = writeLock()) {
            fullyReplicatedOffset = offset;
            writer.write(Long.toString(offset));
        } catch (IOException e) {
            String errorMessage = "Error writing fully replicated offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void takeSnapshot() {
        if (replicaEndOffsets.isEmpty()) {
            return;
        }

        BufferedWriter writer = createWriter(replicaEndOffsetsFile);
        try (writer) {
            // TODO - once done testing, go back to Java serialization
            //writer.write(JavaSerDe.serialize(replicaEndOffsets));
            writer.write(JsonSerDe.serialize(replicaEndOffsets));
            log.debug("Took a snapshot of replica end offsets for table '{}' partition '{}'.", table, partition);
        } catch (IOException e) {
            String errorMessage = "Error taking a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void loadFromSnapshot() {
        if (Files.notExists(Path.of(replicaEndOffsetsFile))) {
            return;
        }

        BufferedReader replicaEndOffsetsReader = createReader(replicaEndOffsetsFile);
        try (replicaEndOffsetsReader) {
            String replicaEndOffsetsLine = replicaEndOffsetsReader.readLine();
            if (replicaEndOffsetsLine != null) {
                // TODO - once done testing, go back to Java serialization
                //replicaEndOffsets.putAll(JavaSerDe.deserialize(replicaEndOffsetsLine));
                replicaEndOffsets.putAll(JsonSerDe.deserialize(replicaEndOffsetsLine, new TypeReference<HashMap<String, Long>>(){}));
                log.debug("Loaded replica end offsets from snapshot for table '{}' partition '{}'.", table, table);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of replica end offsets for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
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
