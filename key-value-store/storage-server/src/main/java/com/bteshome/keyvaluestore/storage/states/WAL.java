package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.Getter;
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

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    @Getter
    private long endIndex;

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.tableName = tableName;
            this.partition = partition;
            String partitionDir = "%s/%s-%s".formatted(storageDirectory, tableName, partition);
            this.logFile = "%s/wal.log".formatted(partitionDir);
            this.endIndex = 0L;
            if (Files.notExists(Path.of(storageDirectory))) {
                Files.createDirectory(Path.of(storageDirectory));
            }
            if (Files.notExists(Path.of(partitionDir))) {
                Files.createDirectory(Path.of(partitionDir));
            }
            if (Files.notExists(Path.of(logFile))) {
                Files.createFile(Path.of(logFile));
            }
            writer = new BufferedWriter(new FileWriter(logFile, true));
            lock = new ReentrantReadWriteLock(true);

            // TODO
            //readLogEndIndex();
        } catch (IOException e) {
            String errorMessage = "Error initializing WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    // TODO - upon restart, if there are already entires,
    //  - populate WAL state
    //  - replay
    /*private void readLogEndIndex() {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile));){
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                long index = Long.parseLong(parts[0]);
                endIndex = Math.max(endIndex, index);
            }
        } catch (IOException e) {
            log.debug("Error reading WAL WAL appended. '{}'='{}' to table '{}' partition '{}'.", key, value, table, partition);

            String errorMessage = "Error reading WAL log file.";
            throw new StorageServerException(e);
        }
    }*/

    public long appendLog(String operation, String key, String value) {
        try (AutoCloseableLock l = writeLock()) {
            endIndex++;
            String logEntry = "%s %s %s %s".formatted(endIndex, operation, key, value);
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
                endIndex++;
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
                String[] parts = line.split(" ");
                long index = Long.parseLong(parts[0]);
                if (index > afterIndex) {
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
