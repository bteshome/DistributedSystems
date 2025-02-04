package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
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
    private final Map<String, LogPosition> replicaEndOffsets;
    private final ConcurrentHashMap<LogPosition, Long> logTimestamps;
    private LogPosition committedOffset;
    private LogPosition previousLeaderEndOffset;
    private final String committedOffsetSnapshotFile;
    private final String previousLeaderEndOffsetFile;
    private final ReentrantReadWriteLock lock;

    public OffsetState(String table, int partition, StorageSettings storageSettings) {
        this.table = table;
        this.partition = partition;
        this.nodeId = storageSettings.getNode().getId();
        committedOffset = LogPosition.ZERO;
        replicaEndOffsets = new ConcurrentHashMap<>();
        logTimestamps = new ConcurrentHashMap<>();
        previousLeaderEndOffset = LogPosition.ZERO;
        lock = new ReentrantReadWriteLock(true);
        committedOffsetSnapshotFile = "%s/%s-%s/committedOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        previousLeaderEndOffsetFile = "%s/%s-%s/previousLeaderEndOffset.ser".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        loadCommittedOffsetFromSnapshot();
        loadPreviousLeaderEndOffset();
    }

    public void trimLogTimestamps() {
        // TODO - should be configurable?
        long timestampTTL = Duration.ofMinutes(15).toMillis();
        for (LogPosition offset : logTimestamps.keySet())
            logTimestamps.compute(offset, (key, value) -> value < System.currentTimeMillis() - timestampTTL ? null : value);
    }

    public void setLogTimestamp(LogPosition offset, long logTimestamp) {
        logTimestamps.put(offset, logTimestamp);
    }

    public long getLogTimestamp(LogPosition offset) {
        return logTimestamps.get(offset);
    }

    public Map<String, LogPosition> getReplicaEndOffsets() {
        return new HashMap<>(replicaEndOffsets);
    }

    public void setReplicaEndOffset(String replicaId, LogPosition offset) {
        replicaEndOffsets.put(replicaId, offset);
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
            if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition)))
                throw new StorageServerException("Illegal access. Not the leader for table '%s' partition '%s'.".formatted(table, partition));
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
