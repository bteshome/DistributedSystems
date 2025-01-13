package com.bteshome.keyvaluestore.storage;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

@Getter
@Setter
@Slf4j
public class WAL {
    private final String logFile;
    private long endIndex;

    public WAL(String storageDirectory, String tableName, int partition) throws IOException {
        this.logFile = "%s/%s-%s.log".formatted(storageDirectory, tableName, partition);
        this.endIndex = 0L;
        if (Files.notExists(Path.of(logFile))) {
            Files.createFile(Path.of(logFile));
        }
        readLogEndIndex();
    }

    private void readLogEndIndex() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                long index = Long.parseLong(parts[0]);
                endIndex = Math.max(endIndex, index);
            }
        }
    }

    public void appendLog(String operation, String key, String base64value) throws IOException {
        endIndex++;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true));) {
            String logEntry = "%s %s %s %s".formatted(endIndex, operation, key, base64value);
            writer.write(logEntry);
            writer.newLine();
        }
    }
}
