package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.JsonSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.entities.OperationType;
import com.bteshome.keyvaluestore.storage.entities.WALEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class WAL implements AutoCloseable {
    private final String logFile;
    private final String tableName;
    private final int partition;
    private final FileChannel writerChannel;
    private final ReentrantReadWriteLock lock;
    private int startLeaderTerm = 0;
    private long startIndex = 0L;
    private int endLeaderTerm = 0;
    private long endIndex = 0L;
    private static final int HEADER_SIZE = 34;
    private static final int BUFFER_SIZE = HEADER_SIZE + 4062;
    private static final ThreadLocal<ByteBuffer> BUFFER_CACHE = ThreadLocal.withInitial(() -> ByteBuffer.allocate(BUFFER_SIZE));

    public WAL(String storageDirectory, String tableName, int partition) {
        try {
            this.tableName = tableName;
            this.partition = partition;
            this.logFile = "%s/%s-%s/wal.bin".formatted(storageDirectory, tableName, partition);
            if (Files.exists(Path.of(logFile)))
                ChecksumUtil.readAndVerify(logFile);
            this.writerChannel = createWriter();
            lock = new ReentrantReadWriteLock(true);
        } catch (IOException e) {
            String errorMessage = "Error initializing WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    // TODO - this needs to be tested
    public void truncateToBeforeInclusive(LogPosition toOffset) {
        try (AutoCloseableLock l = writeLock()) {
            if (toOffset.equals(this.endLeaderTerm, this.endIndex))
                return;

            if (toOffset.isGreaterThan(this.endLeaderTerm, this.endIndex)) {
                throw new StorageServerException("Invalid log position '%s' to truncate WAL file to before. WAL end term is '%s' and index is '%s.".formatted(
                        toOffset,
                        this.endLeaderTerm,
                        this.endIndex));
            }

            ByteBuffer buffer = getByteBuffer();
            long position = 0;

            while (true) {
                buffer.clear();
                buffer.position(0);

                int bytesRead = writerChannel.read(buffer);
                if (bytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(bytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                int entryLength = HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength() + walEntry.indexLength();
                writerChannel.position(writerChannel.position() - bytesRead + entryLength);

                if (walEntry.isLessThanOrEquals(toOffset)) {
                    position = writerChannel.position();
                    continue;
                }

                writerChannel.truncate(position);
                writerChannel.force(true);
                break;
            }

            setEndOffset(toOffset.leaderTerm(), toOffset.index());

            String errorMessage = "Truncated WAL for table '%s' partition '%s' to before offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.info(errorMessage);
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
            log.info("Truncating WAL to after offset {}. Current start offset is {}, end offset is : {}.",
                    toOffset,
                    LogPosition.of(this.startLeaderTerm, this.startIndex),
                    LogPosition.of(this.endLeaderTerm, this.endIndex));

            if (toOffset.isGreaterThanOrEquals(this.endLeaderTerm, this.endIndex)) {
                writerChannel.truncate(0);
                writerChannel.force(true);
                setStartOffset(0, 0L);
                return;
            }

            ByteBuffer buffer = getByteBuffer();

            while (true) {
                buffer.clear();
                buffer.position(0);

                int bytesRead = writerChannel.read(buffer);
                if (bytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(bytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                int entryLength = HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength() + walEntry.indexLength();
                writerChannel.position(writerChannel.position() - bytesRead + entryLength);

                if (walEntry.isLessThanOrEquals(toOffset))
                    continue;

                long truncatePosition = writerChannel.position();
                ByteBuffer buffer2 = ByteBuffer.allocateDirect((int)(writerChannel.size() - truncatePosition));
                writerChannel.read(buffer2);
                buffer2.flip();

                writerChannel.truncate(writerChannel.size() - truncatePosition);

                writerChannel.position(0);
                writerChannel.write(buffer2);
                writerChannel.force(true);

                setStartOffset(walEntry.leaderTerm(), walEntry.index());

                break;
            }

            String errorMessage = "Truncated WAL for table '%s' partition '%s' to after offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.info(errorMessage);
        } catch (Exception e) {
            String errorMessage = "Error truncating WAL for table '%s' partition '%s' to after offset '%s'.".formatted(
                    tableName,
                    partition,
                    toOffset);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<LogPosition> appendPutOperation(int leaderTerm, long timestamp, List<Item> items, long expiryTime) {
        if (items.isEmpty())
            return List.of();

        try (AutoCloseableLock l = writeLock()) {
            ByteBuffer buffer = getByteBuffer();
            List<LogPosition> itemOffsets = new ArrayList<>(items.size());

            for (Item item : items) {
                incrementEndOffset(leaderTerm);

                byte[] keyBytes = item.getKey().getBytes();
                byte[] valueBytes = item.getValue();
                byte[] indexBytes = null;

                int entrySize = HEADER_SIZE + keyBytes.length + valueBytes.length;

                if (item.getIndexKeys() != null && !item.getIndexKeys().isEmpty()) {
                    indexBytes = JsonSerDe.serializeToBytes(item.getIndexKeys());
                    entrySize += indexBytes.length;
                }

                buffer.clear();
                buffer.position(0);
                buffer.limit(entrySize);

                buffer.putInt(leaderTerm);
                buffer.putLong(endIndex);
                buffer.putLong(timestamp);
                buffer.put(OperationType.PUT);
                buffer.putLong(expiryTime);
                buffer.put((byte)keyBytes.length);
                buffer.putShort((short)valueBytes.length);
                buffer.putShort((short)(indexBytes == null ? 0 : indexBytes.length));

                buffer.put(keyBytes);
                buffer.put(valueBytes);
                if (indexBytes != null)
                    buffer.put(indexBytes);

                buffer.flip();
                writerChannel.write(buffer);

                LogPosition offset = LogPosition.of(leaderTerm, endIndex);
                itemOffsets.add(offset);
            }

            writerChannel.force(true);
            log.trace("Appended PUT operation for '{}' items to WAL for table '{}' partition '{}'.", items.size(), tableName, partition);

            return itemOffsets;
        } catch (IOException e) {
            String errorMessage = "Error appending PUT operation to the WAL buffer for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public List<LogPosition> appendDeleteOperation(int leaderTerm, long timestamp, List<String> itemKeys) {
        if (itemKeys.isEmpty())
            return List.of();

        try (AutoCloseableLock l = writeLock()) {
            ByteBuffer buffer = getByteBuffer();
            List<LogPosition> itemOffsets = new ArrayList<>(itemKeys.size());

            for (String key : itemKeys) {
                incrementEndOffset(leaderTerm);

                byte[] keyBytes = key.getBytes();
                int entrySize = HEADER_SIZE + keyBytes.length;

                buffer.clear();
                buffer.position(0);
                buffer.limit(entrySize);

                buffer.putInt(leaderTerm);
                buffer.putLong(endIndex);
                buffer.putLong(timestamp);
                buffer.put(OperationType.DELETE);
                buffer.putLong(0L);
                buffer.put((byte)keyBytes.length);
                buffer.putShort((short)0);
                buffer.putShort((short)0);

                buffer.put(keyBytes);

                buffer.flip();
                writerChannel.write(buffer);

                LogPosition offset = LogPosition.of(leaderTerm, endIndex);
                itemOffsets.add(offset);
            }

            writerChannel.force(true);
            log.trace("Appended DELETE operation for '{}' items to WAL for table '{}' partition '{}'.", itemKeys.size(), tableName, partition);
            return itemOffsets;
        } catch (IOException e) {
            String errorMessage = "Error appending DELETE operation to WAL for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    public void appendLogs(List<WALEntry> logEntries) {
        try (AutoCloseableLock l = writeLock()) {
            ByteBuffer buffer = getByteBuffer();

            for (WALEntry logEntry : logEntries) {
                byte[] keyBytes = logEntry.key();
                byte[] valueBytes = logEntry.value();
                byte[] indexBytes = logEntry.indexes();

                int entrySize = HEADER_SIZE + keyBytes.length + valueBytes.length;
                if (indexBytes != null)
                    entrySize += indexBytes.length;

                buffer.clear();
                buffer.position(0);
                buffer.limit(entrySize);

                buffer.putInt(logEntry.leaderTerm());
                buffer.putLong(logEntry.index());
                buffer.putLong(logEntry.timestamp());
                buffer.put(logEntry.operation());
                buffer.putLong(logEntry.expiryTime());
                buffer.put((byte)keyBytes.length);
                buffer.putShort((short)valueBytes.length);
                buffer.putShort((short)(indexBytes == null ? 0 : indexBytes.length));

                buffer.put(keyBytes);
                buffer.put(valueBytes);
                if (indexBytes != null)
                    buffer.put(indexBytes);

                buffer.flip();
                writerChannel.write(buffer);
            }

            writerChannel.force(true);
            if (startLeaderTerm == 0 && startIndex == 0)
                setStartOffset(logEntries.getFirst().leaderTerm(), logEntries.getFirst().index());
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
            FileChannel readerChannel = createReaderChannel()) {
            ByteBuffer buffer = getByteBuffer();
            List<WALEntry> entries = new ArrayList<>();

            buffer.clear();
            buffer.position(0);

            while (entries.size() < limit) {
                buffer.clear();
                buffer.position(0);

                int bytesRead = readerChannel.read(buffer);
                if (bytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(bytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                int entryLength = HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength() + walEntry.indexLength();
                readerChannel.position(readerChannel.position() - bytesRead + entryLength);

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
             FileChannel readerChannel = createReaderChannel()) {
            ByteBuffer buffer = getByteBuffer();
            List<WALEntry> entries = new ArrayList<>();

            while (true) {
                buffer.clear();
                buffer.position(0);

                int bytesRead = readerChannel.read(buffer);
                if (bytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(bytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                int entryLength = HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength() + walEntry.indexLength();
                readerChannel.position(readerChannel.position() - bytesRead + entryLength);

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
        try (AutoCloseableLock l = writeLock();
            FileChannel readerChannel = createReaderChannel()) {
            ByteBuffer buffer = getByteBuffer();
            List<WALEntry> entries = new ArrayList<>();

            buffer.clear();
            buffer.position(0);

            while (true) {
                buffer.clear();
                buffer.position(0);

                int bytesRead = readerChannel.read(buffer);
                if (bytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(bytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                int entryLength = HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength() + walEntry.indexLength();
                readerChannel.position(readerChannel.position() - bytesRead + entryLength);

                entries.add(walEntry);
            }

            if (!entries.isEmpty()) {
                setStartOffset(entries.getFirst().leaderTerm(), entries.getFirst().index());
                setEndOffset(entries.getLast().leaderTerm(), entries.getLast().index());
            }

            return entries;
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
             FileChannel readerChannel = createReaderChannel()) {
            ByteBuffer buffer = getByteBuffer();
            List<WALEntry> entries = new ArrayList<>();
            long difference = 0L;

            buffer.clear();
            buffer.position(0);

            while (true) {
                buffer.clear();
                buffer.position(0);

                int headerBytesRead = readerChannel.read(buffer);
                if (headerBytesRead <= 0)
                    break;

                buffer.flip();
                buffer.limit(headerBytesRead);

                WALEntry walEntry = WALEntry.fromByteBuffer(buffer);
                readerChannel.position(readerChannel.position() - headerBytesRead + HEADER_SIZE + walEntry.keyLength() + walEntry.valueLength());

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
            if (writerChannel != null) {
                writerChannel.force(true);
                writerChannel.close();
            }
            if (Files.exists(Path.of(logFile)))
                ChecksumUtil.generateAndWrite(logFile);
        } catch (Exception e) {
            String errorMessage = "Error closing WAL file for table '%s' partition '%s'.".formatted(tableName, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void incrementEndOffset(int leaderTerm) {
        if (leaderTerm == this.endLeaderTerm) {
            this.endIndex++;
        } else if (leaderTerm > this.endLeaderTerm) {
            this.endLeaderTerm = leaderTerm;
            this.endIndex = 1;
        } else {
            throw new StorageServerException("Invalid leader term '%s' for WAL entry. Current leader term is '%s'."
                    .formatted(leaderTerm, this.endLeaderTerm));
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

    private FileChannel createWriter() throws IOException {
        return FileChannel.open(
                Path.of(this.logFile),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
    }

    private FileChannel createReaderChannel() throws IOException {
        return FileChannel.open(
                Path.of(this.logFile),
                StandardOpenOption.READ);
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    private static ByteBuffer getByteBuffer() {
        //ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        ByteBuffer buffer = BUFFER_CACHE.get();
        buffer.clear();
        return buffer;
    }
}
