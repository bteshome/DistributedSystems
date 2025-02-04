package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
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
    private int startLeaderTerm = 0;
    private long startIndex = 0L;
    private int endLeaderTerm = 0;
    private long endIndex = 0L;

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.tableName = tableName;
            this.partition = partition;
            this.logFile = "%s/%s-%s/wal.log".formatted(storageDirectory, tableName, partition);
            if (Files.notExists(Path.of(logFile)))
                Files.createFile(Path.of(logFile));
            else
                ChecksumUtil.readAndVerify(logFile);
            writer = new BufferedWriter(new FileWriter(logFile, true));
            lock = new ReentrantReadWriteLock(true);
        } catch (IOException e) {
            String errorMessage = "Error initializing WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    // TODO - this needs to be tested
    public void truncateToBeforeInclusive(LogPosition toOffset) {
        try (AutoCloseableLock l = writeLock();
             RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            if (toOffset.equals(this.endLeaderTerm, this.endIndex))
                return;

            if (toOffset.isGreaterThan(this.endLeaderTerm, this.endIndex)) {
                throw new StorageServerException("Invalid log position '%s' to truncate WAL file to before. WAL end term is '%s' and index is '%s.".formatted(
                        toOffset,
                        this.endLeaderTerm,
                        this.endIndex));
            }

            writer.flush();

            try {
                writer.close();

                String line;
                long position = 0;

                while ((line = raf.readLine()) != null) {
                    String[] parts = line.split(" ");
                    int term = Integer.parseInt(parts[0]);
                    long index = Long.parseLong(parts[1]);

                    if (toOffset.isGreaterThanOrEquals(term, index)) {
                        position = channel.position();
                        continue;
                    }

                    channel.truncate(position);
                    break;
                }

                setEndOffset(toOffset.leaderTerm(), toOffset.index());

                String errorMessage = "Truncated WAL for table '%s' partition '%s' to before offset '%s'.".formatted(
                        tableName,
                        partition,
                        toOffset);
                log.info(errorMessage);
            } finally {
                try {
                    writer = new BufferedWriter(new FileWriter(logFile, true));
                } catch (IOException e) {
                    String errorMessage = "Error recreating WAL writer for table '%s' partition '%s' after truncating to before offset '%s'.".formatted(
                            tableName,
                            partition,
                            toOffset);
                    log.error(errorMessage, e);
                }
            }
        } catch (IOException e) {
            String errorMessage = "Error truncating WAL for table '%s' partition '%s' to before offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    // TODO - test this
    public void truncateToAfterExclusive(LogPosition toOffset) {
        if (toOffset.equals(LogPosition.ZERO))
            return;

        try (AutoCloseableLock l = writeLock()) {
            if (toOffset.isGreaterThan(this.endLeaderTerm, this.endIndex)) {
                throw new StorageServerException("Invalid log position '%s' to truncate WAL file to. WAL end term is '%s' and index is '%s'.".formatted(
                        toOffset,
                        this.endLeaderTerm,
                        this.endIndex));
            }

            writer.flush();

            try {
                writer.close();

                if (toOffset.equals(this.endLeaderTerm, this.endIndex)) {
                    try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
                         FileChannel channel = raf.getChannel()) {
                        channel.truncate(0);
                        // TODO - this needs to be tested.
                        setStartOffset(0, 0L);
                        return;
                    }
                }

                try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
                     FileChannel channel = raf.getChannel()) {
                    String line;
                    long position = 0;

                    while ((line = raf.readLine()) != null) {
                        String[] parts = line.split(" ");
                        int term = Integer.parseInt(parts[0]);
                        long index = Long.parseLong(parts[1]);

                        if (toOffset.isGreaterThanOrEquals(term, index)) {
                            position = channel.position();
                            continue;
                        }

                        setStartOffset(term, index);
                        break;
                    }

                    channel.position(position);
                    ByteBuffer buffer = ByteBuffer.allocate((int) (channel.size() - position - 1));
                    channel.read(buffer);
                    buffer.flip();
                    channel.position(0);
                    channel.write(buffer);
                    channel.truncate(buffer.limit());

                    String errorMessage = "Truncated WAL for table '%s' partition '%s' to after offset '%s'.".formatted(
                            tableName,
                            partition,
                            toOffset);
                    log.info(errorMessage);
                }
            } finally {
                try {
                    writer = new BufferedWriter(new FileWriter(logFile, true));
                } catch (IOException e) {
                    String errorMessage = "Error recreating WAL writer for table '%s' partition '%s' after truncating to after offset '%s'.".formatted(
                            tableName,
                            partition,
                            toOffset);
                    log.error(errorMessage, e);
                }
            }
        } catch (Exception e) {
            String errorMessage = "Error truncating WAL for table '%s' partition '%s' to after offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public long appendPutOperation(int leaderTerm, long timestamp, List<Item> items, Instant expiryTime) {
        if (items.isEmpty())
            return endIndex;

        try (AutoCloseableLock l = writeLock()) {
            for (Item item : items) {
                incrementEndOffset(leaderTerm);
                String logEntry = new WALEntry(leaderTerm, endIndex, timestamp, OperationType.PUT, item.getKey(), item.getValue(), expiryTime).toString();
                writer.write(logEntry);
                writer.newLine();
            }
            log.trace("Appended PUT operation for '{}' items to the WAL buffer for table '{}' partition '{}'.", items.size(), tableName, partition);
            return endIndex;
        } catch (IOException e) {
            String errorMessage = "Error appending PUT operation to the WAL buffer for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public long appendDeleteOperation(int leaderTerm, long timestamp, List<ItemKey> items) {
        if (items.isEmpty())
            return endIndex;

        try (AutoCloseableLock l = writeLock()) {
            for (ItemKey itemKey : items) {
                incrementEndOffset(leaderTerm);
                String logEntry = new WALEntry(leaderTerm, endIndex, timestamp, OperationType.DELETE, itemKey.keyString(), null, null).toString();
                writer.write(logEntry);
                writer.newLine();
            }
            log.trace("Appended DELETE operation for '{}' items to WAL for table '{}' partition '{}'.", items.size(), tableName, partition);
            return endIndex;
        } catch (IOException e) {
            String errorMessage = "Error appending DELETE operation to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void appendLogs(List<WALEntry> logEntries) {
        try (AutoCloseableLock l = writeLock()) {
            for (WALEntry logEntry : logEntries) {
                writer.write(logEntry.toString());
                writer.newLine();
            }
            writer.flush();
            setEndOffset(logEntries.getLast().leaderTerm(), logEntries.getLast().index());
            log.trace("'{}' log entries appended for table '{}' partition '{}'.", logEntries.size(), tableName, partition);
        } catch (IOException e) {
            String errorMessage = "Error appending entries to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<WALEntry> readLogs(LogPosition afterOffset, int limit) {
        try (AutoCloseableLock l = readLock();
            BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<WALEntry> entries = new ArrayList<>();

            while (entries.size() < limit && (line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.isLessThanOrEquals(afterOffset))
                    continue;
                entries.add(walEntry);
            }

            return entries;
        } catch (IOException e) {
            String errorMessage = "Error reading WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<WALEntry> readLogs(LogPosition afterOffset, LogPosition upToOffsetInclusive) {
        try (AutoCloseableLock l = readLock();
             BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<WALEntry> entries = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.isLessThanOrEquals(afterOffset))
                    continue;
                if (walEntry.isGreaterThan(upToOffsetInclusive))
                    break;
                entries.add(walEntry);
            }

            return entries;
        } catch (IOException e) {
            String errorMessage = "Error reading WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<WALEntry> loadFromFile() {
        try (AutoCloseableLock l = writeLock()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
                String line;
                List<WALEntry> entries = new ArrayList<>();

                while ((line = reader.readLine()) != null) {
                    WALEntry walEntry = WALEntry.fromString(line);
                    entries.add(walEntry);
                }

                if (!entries.isEmpty()) {
                    setStartOffset(entries.getFirst().leaderTerm(), entries.getFirst().index());
                    setEndOffset(entries.getLast().leaderTerm(), entries.getLast().index());
                }

                return entries;
            }
        } catch (IOException e) {
            String errorMessage = "Error loading from WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public LogPosition getStartOffset() {
        try (AutoCloseableLock l = readLock()) {
            return LogPosition.of(startLeaderTerm, startIndex);
        }
    }

    public LogPosition getEndOffset() {
        try (AutoCloseableLock l = readLock()) {
            return LogPosition.of(endLeaderTerm, endIndex);
        }
    }

    public void setEndOffset(LogPosition offset) {
        try (AutoCloseableLock l = writeLock()) {
            this.endLeaderTerm = offset.leaderTerm();
            this.endIndex = offset.index();
        }
    }

    public long getLag(LogPosition logPosition, LogPosition comparedTo) {
        if (logPosition.isGreaterThanOrEquals(comparedTo))
            return 0L;

        if (logPosition.leaderTerm() == comparedTo.leaderTerm())
            return comparedTo.index() - logPosition.index();

        try (AutoCloseableLock l = readLock();
             BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            long difference = 0L;

            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.isLessThan(logPosition))
                    continue;
                if (walEntry.isGreaterThanOrEquals(comparedTo))
                    break;
                difference++;
            }

            return difference;
        } catch (IOException e) {
            String errorMessage = "Error comparing WAL offsets for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            if (Files.exists(Path.of(logFile)))
                ChecksumUtil.generateAndWrite(logFile);
        } catch (IOException e) {
            String errorMessage = "Error closing WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void flush() {
        try {
            writer.flush();
        } catch (IOException e) {
            String errorMessage = "Error flushing WAL entries for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
        }
    }

    private void incrementEndOffset(int leaderTerm) {
        if (leaderTerm == this.endLeaderTerm) {
            this.endIndex++;
        } else if (leaderTerm == this.endLeaderTerm + 1) {
            this.endLeaderTerm = leaderTerm;
            this.endIndex = 1;
        } else {
            throw new StorageServerException("Invalid leader term '%s' for WAL entry. Expected '%s' or '%s'."
                    .formatted(leaderTerm, this.endLeaderTerm, this.endLeaderTerm + 1));
        }

        if (this.startLeaderTerm == 0 && this.startIndex == 0) {
            this.startLeaderTerm = this.endLeaderTerm;
            this.startIndex = this.endIndex;
        }
    }

    private void setEndOffset(int leaderTerm, long index) {
        this.endLeaderTerm = leaderTerm;
        this.endIndex = index;
    }

    private void setStartOffset(int leaderTerm, long index) {
        this.startLeaderTerm = leaderTerm;
        this.startIndex = index;
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }
}
