package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private int startLeaderTerm = 0;
    private long startIndex = 0L;
    private int endLeaderTerm = 0;
    private long endIndex = 0L;
    private LogPosition previousLeaderEndOffset = LogPosition.empty();

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.tableName = tableName;
            this.partition = partition;
            this.logFile = "%s/%s-%s/wal.log".formatted(storageDirectory, tableName, partition);
            if (Files.notExists(Path.of(logFile)))
                Files.createFile(Path.of(logFile));
            writer = new BufferedWriter(new FileWriter(logFile, true));
            lock = new ReentrantReadWriteLock(true);
        } catch (IOException e) {
            String errorMessage = "Error initializing WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void truncateToBeforeInclusive(LogPosition toOffset) {
        try (AutoCloseableLock l = writeLock();
             RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            if (toOffset.equals(this.endLeaderTerm, this.endIndex))
                return;

            if (toOffset.isGreaterThan(this.endLeaderTerm, this.endIndex)) {
                throw new StorageServerException("Invalid log position '%s' to truncate WAL file to. WAL end term is '%s' and index is '%s.".formatted(
                        toOffset,
                        this.endLeaderTerm,
                        this.endIndex));
            }

            String line;
            long position = 0;

            while ((line = raf.readLine()) != null) {
                String[] parts = line.split(" ");
                int term = Integer.parseInt(parts[0]);
                long index = Long.parseLong(parts[1]);

                if (toOffset.isGreaterThanOrEquals(term, index)) {
                    position = channel.position();
                    this.endLeaderTerm = term;
                    this.endIndex = index;
                    continue;
                }

                channel.truncate(position);
                break;
            }

            String errorMessage = "Truncated WAL for table '%s' partition '%s' to offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.info(errorMessage);
        } catch (IOException e) {
            String errorMessage = "Error truncating WAL for table '%s' partition '%s' to offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void truncateToAfterExclusive(LogPosition toOffset) {
        if (toOffset.equals(LogPosition.empty()))
            return;

        try (AutoCloseableLock l = writeLock()) {
            if (toOffset.equals(this.endLeaderTerm, this.endIndex)) {
                try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
                     FileChannel channel = raf.getChannel()) {
                    channel.truncate(0);
                    startLeaderTerm = endLeaderTerm;
                    startIndex = endIndex;
                    return;
                }
            }

            if (toOffset.isGreaterThan(this.endLeaderTerm, this.endIndex)) {
                throw new StorageServerException("Invalid log position '%s' to truncate WAL file to. WAL end term is '%s' and index is '%s'.".formatted(
                        toOffset,
                        this.endLeaderTerm,
                        this.endIndex));
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

                    startLeaderTerm = term;
                    startIndex = index;
                    break;
                }

                channel.position(position);
                ByteBuffer buffer = ByteBuffer.allocate((int) (channel.size() - position - 1));
                channel.read(buffer);
                buffer.flip();
                channel.position(0);
                channel.write(buffer);
                channel.truncate(buffer.limit());

                String errorMessage = "Truncated WAL for table '%s' partition '%s' to offset '%s'."
                        .formatted(tableName, partition, toOffset);
                log.info(errorMessage);
            }
        } catch (Exception e) {
            String errorMessage = "Error truncating WAL for table '%s' partition '%s' to offset '%s'."
                    .formatted(tableName, partition, toOffset);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public long appendItems(int leaderTerm, String operation, List<Item> items) {
        if (items.isEmpty())
            return endIndex;

        try (AutoCloseableLock l = writeLock()) {
            for (Item item : items) {
                incrementIndex(leaderTerm);
                String logEntry = new WALEntry(leaderTerm, endIndex, operation, item.getKey(), item.getValue()).toString();
                writer.write(logEntry);
                writer.newLine();
            }
            writer.flush();
            log.trace("Operation '{}' for '{}' items appended to WAL for table '{}' partition '{}'.", operation, items.size(), tableName, partition);
            return endIndex;
        } catch (IOException e) {
            String errorMessage = "Error appending items to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public LogPosition appendLogs(List<WALEntry> logEntries) {
        try (AutoCloseableLock l = writeLock()) {
            for (WALEntry logEntry : logEntries) {
                writer.write(logEntry.toString());
                writer.newLine();
            }
            writer.flush();
            this.endLeaderTerm = logEntries.getLast().leaderTerm();
            this.endIndex = logEntries.getLast().index();
            log.trace("'{}' log entries appended for table '{}' partition '{}'.", logEntries.size(), tableName, partition);
            return LogPosition.of(endLeaderTerm, endIndex);
        } catch (IOException e) {
            String errorMessage = "Error appending entries to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<WALEntry> readLogs() {
        try (AutoCloseableLock l = readLock();
             BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<WALEntry> lines = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                lines.add(walEntry);
            }

            return lines;
        } catch (IOException e) {
            String errorMessage = "Error reading WAL for table '%s' partition '%s'.".formatted(tableName, partition);
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

    public List<WALEntry> loadFromFile() {
        try (AutoCloseableLock l = writeLock();
             BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;
            List<WALEntry> entries = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                entries.add(walEntry);
            }

            if (!entries.isEmpty()) {
                this.startLeaderTerm = entries.getFirst().leaderTerm();
                this.startIndex = entries.getFirst().index();
                this.endLeaderTerm = entries.getLast().leaderTerm();
                this.endIndex = entries.getLast().index();
            }

            return entries;
        } catch (IOException e) {
            String errorMessage = "Error loading from WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    /*public LogPosition getStartOffset() {
        try (AutoCloseableLock l = readLock()) {
            BufferedReader reader = new BufferedReader(new FileReader(logFile));
            String line = reader.readLine();
            if (line == null)
                return LogPosition.empty();
            WALEntry walEntry = WALEntry.fromString(line);
            return LogPosition.of(walEntry.leaderTerm(), walEntry.index());
        } catch (IOException e) {
            String errorMessage = "Error reading WAL start offset for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }*/

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

    public LogPosition getPreviousLeaderEndOffset() {
        try (AutoCloseableLock l = readLock();) {
            return previousLeaderEndOffset;
        }
    }

    public void setPreviousLeaderEndOffset(LogPosition offset) {
        try (AutoCloseableLock l = writeLock();) {
            previousLeaderEndOffset = offset;
        }
    }

    /*public long getEndIndexForLeaderTerm(int leaderTerm) {
        try (AutoCloseableLock l = readLock();
            BufferedReader reader = new BufferedReader(new FileReader(logFile));) {
            String line;

            long index = 0L;
            while ((line = reader.readLine()) != null) {
                WALEntry walEntry = WALEntry.fromString(line);
                if (walEntry.leaderTerm() > leaderTerm)
                    break;
                if (walEntry.leaderTerm() == leaderTerm)
                    index = walEntry.index();
            }

            return index;
        } catch (IOException e) {
            String errorMessage = "Error reading leader term end index for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }*/

    /*public long getLag(LogPosition logPosition, LogPosition comparedTo) {
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
                if (walEntry.isGreaterThan(comparedTo))
                    break;
                difference++;
            }

            return difference;
        } catch (IOException e) {
            String errorMessage = "Error comparing WAL offsets for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }*/

    @Override
    public void close() {
        try {
            if (writer != null)
                writer.close();
        } catch (IOException e) {
            String errorMessage = "Error closing WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        } finally {
            writer = null;
        }
    }

    private void incrementIndex(int leaderTerm) {
        if (leaderTerm == this.endLeaderTerm) {
            this.endIndex++;
        } else if (leaderTerm == this.endLeaderTerm + 1) {
            this.endLeaderTerm = leaderTerm;
            this.endIndex = 1;
        } else {
            throw new StorageServerException("Invalid leader term '%s' for WAL entry. Expected '%s' or '%s'."
                    .formatted(leaderTerm, this.endLeaderTerm, this.endLeaderTerm + 1));
        }
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }
}
