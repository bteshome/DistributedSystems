package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class WAL implements AutoCloseable {
    private final String logFile;
    private final String tableName;
    private final int partition;
    private BufferedWriter writer;
    private final ReentrantReadWriteLock lock;
    private long endIndex;
    private int endLeaderTerm;

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.tableName = tableName;
            this.partition = partition;
            this.logFile = "%s/%s-%s/wal.log".formatted(storageDirectory, tableName, partition);
            this.endIndex = 0L;
            this.endLeaderTerm = 0;
            if (Files.notExists(Path.of(logFile))) {
                Files.createFile(Path.of(logFile));
            }
            writer = new BufferedWriter(new FileWriter(logFile, true));
            lock = new ReentrantReadWriteLock(true);
        } catch (IOException e) {
            String errorMessage = "Error initializing WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public long appendLog(int leaderTerm, String operation, String key, String value) {
        try (AutoCloseableLock l = writeLock()) {
            this.endIndex++;
            this.endLeaderTerm = leaderTerm;
            String logEntry = new WALEntry(endIndex, leaderTerm, operation, key, value).toString();
            writer.write(logEntry);
            writer.newLine();
            writer.flush();
            log.trace("WAL appended. '{}'='{}' to table '{}' partition '{}'.", key, value, tableName, partition);
            return endIndex;
        } catch (IOException e) {
            String errorMessage = "Error appending entry to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void appendLogs(List<String> logEntries) {
        try (AutoCloseableLock l = writeLock()) {
            for (String logEntry : logEntries) {
                WALEntry walEntry = WALEntry.fromString(logEntry);
                this.endIndex = walEntry.getIndex();
                this.endLeaderTerm = walEntry.getLeaderTerm();
                writer.write(logEntry);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            String errorMessage = "Error appending entries to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<String> readLog(long afterIndex, int limit) {
        try (AutoCloseableLock l = readLock();
            BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<String> lines = new ArrayList<>();

            while (lines.size() < limit && (line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.getIndex() > afterIndex) {
                    lines.add(line);
                }
            }

            return lines;
        } catch (IOException e) {
            String errorMessage = "Error reading WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public long getEndIndex() {
        try (AutoCloseableLock l = readLock()) {
            return endIndex;
        }
    }

    public void setEndIndex(long index) {
        try (AutoCloseableLock l = writeLock()) {
            endIndex = index;
        }
    }

    public int getEndLeaderTerm() {
        try (AutoCloseableLock l = readLock()) {
            return endLeaderTerm;
        }
    }

    public long getEndIndexForLeaderTerm(int leaderTerm) {
        try (AutoCloseableLock l = readLock();
            BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;

            long index = 0L;
            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.getLeaderTerm() > leaderTerm) {
                    break;
                }
                if (walEntry.getLeaderTerm() == leaderTerm) {
                    index = walEntry.getIndex();
                }
            }

            return index;
        } catch (IOException e) {
            String errorMessage = "Error reading WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            String errorMessage = "Error closing WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        } finally {
            writer = null;
        }
    }
}
