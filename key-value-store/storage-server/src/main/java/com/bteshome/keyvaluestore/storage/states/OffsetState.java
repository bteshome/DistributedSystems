package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.Utils;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.CompressionUtil;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class OffsetState {
    private final String table;
    private final int partition;
    private final String nodeId;
    private final Map<String, Tuple<LogPosition, Long>> replicaEndOffsets;
    private final Map<String, LogPosition> replicaCommittedOffsets;
    private LogPosition committedOffset;
    private LogPosition endOffset;
    private LogPosition previousLeaderEndOffset;
    private final String committedOffsetSnapshotFile;
    private final String endOffsetSnapshotFile;
    private final String previousLeaderEndOffsetFile;
    private final ReentrantReadWriteLock lock;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        committedOffset = LogPosition.ZERO;
        endOffset = LogPosition.ZERO;
        previousLeaderEndOffset = LogPosition.ZERO;
        replicaEndOffsets = new ConcurrentHashMap<>();
        replicaCommittedOffsets = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
        committedOffsetSnapshotFile = "%s/%s-%s/committedOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        endOffsetSnapshotFile = "%s/%s-%s/endOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        previousLeaderEndOffsetFile = "%s/%s-%s/previousLeaderEndOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        loadCommittedOffsetFromSnapshot();
        loadEndOffsetFromSnapshot();
        loadPreviousLeaderEndOffset();
    }

    public LogPosition getEndOffset() {
        try (AutoCloseableLock l = writeLock()) {
            return endOffset;
        }
    }

    public void setEndOffset(LogPosition offset) {
        try (AutoCloseableLock l = writeLock()) {
            this.endOffset = offset;
            CompressionUtil.compressAndWrite(endOffsetSnapshotFile, offset);
            ChecksumUtil.generateAndWrite(endOffsetSnapshotFile);
            log.trace("Persisted end offset '{}' for table '{}' partition '{}'.", offset, table, partition);
        } catch (Exception e) {
            String errorMessage = "Error writing end offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public Map<String, Tuple<LogPosition, Long>> getReplicaEndOffsets() {
        return new HashMap<>(replicaEndOffsets);
    }

    public LogPosition getReplicaEndOffset(String replicaId) {
        if (!replicaEndOffsets.containsKey(replicaId))
            return null;
        return replicaEndOffsets.get(replicaId).first();
    }

    public void setReplicaEndOffset(String replicaId, LogPosition offset) {
        replicaEndOffsets.put(replicaId, Tuple.of(offset, System.currentTimeMillis()));
    }

    public LogPosition getReplicaCommittedOffset(String replicaId) {
        return replicaCommittedOffsets.getOrDefault(replicaId, null);
    }

    public void setReplicaCommittedOffset(String replicaId, LogPosition offset) {
        replicaCommittedOffsets.put(replicaId, offset);
    }

    public LogPosition getCommittedOffset() {
        try (AutoCloseableLock l = readLock()) {
            return committedOffset;
        }
    }

    public void setCommittedOffset(LogPosition offset) {
        try (AutoCloseableLock l = writeLock()) {
            committedOffset = offset;
            CompressionUtil.compressAndWrite(committedOffsetSnapshotFile, offset);
            ChecksumUtil.generateAndWrite(committedOffsetSnapshotFile);
            log.trace("Persisted committed offset '{}' for table '{}' partition '{}'.", committedOffset, table, partition);
        } catch (Exception e) {
            String errorMessage = "Error writing committed offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public LogPosition getPreviousLeaderEndOffset() {
        try (AutoCloseableLock l = readLock()) {
            return previousLeaderEndOffset;
        }
    }

    public void setPreviousLeaderEndOffset(LogPosition offset) {
        try (AutoCloseableLock l = writeLock()) {
            previousLeaderEndOffset = offset;
            CompressionUtil.compressAndWrite(previousLeaderEndOffsetFile, offset);
            ChecksumUtil.generateAndWrite(previousLeaderEndOffsetFile);
            log.info("Persisted previous leader end offset '{}' for table '{}' partition '{}'.", previousLeaderEndOffset, table, partition);
        } catch (Exception e) {
            String errorMessage = "Error writing previous leader end offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void clearPreviousLeaderEndOffset() {
        try (AutoCloseableLock l = writeLock()) {
            if (!previousLeaderEndOffset.equals(LogPosition.ZERO)) {
                previousLeaderEndOffset = LogPosition.ZERO;
                Files.deleteIfExists(Path.of(previousLeaderEndOffsetFile));
                Files.deleteIfExists(Path.of(previousLeaderEndOffsetFile + ".md5"));
                log.info("Deleted previous leader end offset file '{}' for table '{}' partition '{}'.", previousLeaderEndOffsetFile, table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error deleting previous leader end offset file for table '%s' partition '%s'.".formatted(table, partition);
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
                committedOffset = CompressionUtil.readAndDecompress(committedOffsetSnapshotFile, LogPosition.class);
                log.debug("Loaded replica committed offset from snapshot for table '{}' partition '{}'.", table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of committed offset for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void loadEndOffsetFromSnapshot() {
        if (Files.notExists(Path.of(endOffsetSnapshotFile)))
            return;

        ChecksumUtil.readAndVerify(endOffsetSnapshotFile);

        BufferedReader reader = Utils.createReader(endOffsetSnapshotFile);
        try (reader) {
            String line = reader.readLine();
            if (line != null) {
                this.endOffset = CompressionUtil.readAndDecompress(endOffsetSnapshotFile, LogPosition.class);
                log.debug("Loaded end offset from snapshot for table '{}' partition '{}'.", table, partition);
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from a snapshot of end offset for table '%s' partition '%s'.".formatted(table, partition);
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
                previousLeaderEndOffset = CompressionUtil.readAndDecompress(previousLeaderEndOffsetFile, LogPosition.class);
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
