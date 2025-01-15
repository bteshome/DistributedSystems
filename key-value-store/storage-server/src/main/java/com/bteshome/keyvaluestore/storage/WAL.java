package com.bteshome.keyvaluestore.storage;

import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class WAL implements AutoCloseable {
    private final String logFile;
    private BufferedWriter writer;
    @Getter
    private long endIndex;

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.logFile = "%s/wal/%s-%s.log".formatted(storageDirectory, tableName, partition);
            this.endIndex = 0L;
            if (Files.notExists(Path.of(storageDirectory))) {
                Files.createDirectory(Path.of(storageDirectory));
            }
            if (Files.notExists(Path.of(storageDirectory + "/wal"))) {
                Files.createDirectory(Path.of(storageDirectory + "/wal"));
            }
            if (Files.notExists(Path.of(logFile))) {
                Files.createFile(Path.of(logFile));
            }
            writer = new BufferedWriter(new FileWriter(logFile, true));
            readLogEndIndex();
        } catch (IOException e) {
            throw new StorageServerException(e);
        }
    }

    private void readLogEndIndex() {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile));){
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                long index = Long.parseLong(parts[0]);
                endIndex = Math.max(endIndex, index);
            }
        } catch (IOException e) {
            throw new StorageServerException(e);
        }
    }

    public long appendLog(String operation, String key, String base64value) {
        try {
            endIndex++;
            String logEntry = "%s %s %s %s".formatted(endIndex, operation, key, base64value);
            writer.write(logEntry);
            writer.newLine();
            writer.flush();
            return endIndex;
        } catch (IOException e) {
            throw new StorageServerException(e);
        }
    }

    public void appendLogs(List<String> logEntries) {
        try {
            for (String logEntry : logEntries) {
                endIndex++;
                writer.write(logEntry);
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            throw new StorageServerException(e);
        }
    }

    public List<String> readLog(long afterIndex) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<String> lines = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                long index = Long.parseLong(parts[0]);
                if (index > afterIndex) {
                    lines.add(line);
                }
            }

            return lines;
        } catch (IOException e) {
            throw new StorageServerException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            throw new StorageServerException(e);
        } finally {
            writer = null;
        }
    }
}
