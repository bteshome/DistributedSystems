package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.responses.*;
import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.CompressionUtil;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchPayloadType;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class PartitionState implements AutoCloseable {
    private final String table;
    private final int partition;
    private final Duration timeToLive;
    private final String nodeId;
    private final ConcurrentHashMap<ItemKey, String> data;
    @Getter
    private final PriorityQueue<ItemKey> dataExpiryTimes;
    @Getter
    private final WAL wal;
    @Getter
    private final OffsetState offsetState;
    private final ReentrantReadWriteLock dataLock;
    private final ReentrantReadWriteLock operationLock;
    private final StorageSettings storageSettings;
    private final ISRSynchronizer isrSynchronizer;
    private final String dataSnapshotFile;

    public PartitionState(String table,
                          int partition,
                          StorageSettings storageSettings,
                          ISRSynchronizer isrSynchronizer) {
        this.table = table;
        this.partition = partition;
        this.timeToLive = MetadataCache.getInstance().getTableTimeToLive(table, partition);
        this.nodeId = storageSettings.getNode().getId();
        dataLock = new ReentrantReadWriteLock(true);
        operationLock = new ReentrantReadWriteLock(true);
        data = new ConcurrentHashMap<>();
        dataExpiryTimes = new PriorityQueue<>(Comparator.comparing(ItemKey::expiryTime));
        this.storageSettings = storageSettings;
        this.isrSynchronizer = isrSynchronizer;
        createPartitionDirectoryIfNotExists();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        dataSnapshotFile = "%s/%s-%s/data.ser.snappy".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        offsetState = new OffsetState(table, partition, storageSettings);
        loadFromDataSnapshotAndWALFile();
    }

    public void deleteExpiredItems() {
        List<ItemKey> itemsToRemove = new ArrayList<>();
        Instant now = Instant.now();

        try (AutoCloseableLock l = writeDataLock()) {
            while (!dataExpiryTimes.isEmpty() &&
                   (dataExpiryTimes.peek().expiryTime().isBefore(now) ||
                    dataExpiryTimes.peek().expiryTime().equals(now))) {
                ItemKey itemKey = dataExpiryTimes.poll();
                itemsToRemove.add(itemKey);
            }
        }

        if (!itemsToRemove.isEmpty()) {
            log.debug("Expiring '{}' items.", itemsToRemove.size());
            deleteItemsByItemKey(itemsToRemove);
        }
    }

    public ResponseEntity<ItemPutResponse> putItems(final List<Item> items) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        log.debug("Received PUT request for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

        int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
        LogPosition offset;
        Instant expiryTime = (timeToLive == null || timeToLive.equals(Duration.ZERO)) ? null : Instant.now().plus(timeToLive);
        long now = System.currentTimeMillis();

        try {
            try (AutoCloseableLock l = writeOperationLock()) {
                long index = wal.appendPutOperation(leaderTerm, now, items, expiryTime);
                offset = LogPosition.of(leaderTerm, index);
            }
            offsetState.setLogTimestamp(offset, now);
        } catch (Exception e) {
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build());
        }

        final long timeoutMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        final long start = System.nanoTime();
        log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);

        CompletableFuture.runAsync(() -> {
            boolean committed = false;

            while (System.nanoTime() - start < timeoutNanos) {
                if (offsetState.getCommittedOffset().isGreaterThanOrEquals(offset)) {
                    try (AutoCloseableLock l2 = writeDataLock()) {
                        for (Item item : items) {
                            ItemKey itemKey = ItemKey.of(item.getKey(), expiryTime);
                            data.put(itemKey, item.getValue());
                            if (expiryTime != null)
                                dataExpiryTimes.offer(itemKey);
                        }
                    }

                    log.debug("Successfully committed PUT operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);
                    committed = true;
                    break;
                }

                // TODO - this shouldn't be hard coded
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Error waiting for replicas to acknowledge the log entry.", e);
                }
            }

            if (!committed) {
                String errorMessage = "PUT operation for '{}' items to table '{}' partition '{}' was not committed in the expected time.".formatted(
                        items.size(),
                        table,
                        partition);
                log.error(errorMessage);
            }
        });

        return ResponseEntity.ok().body(ItemPutResponse.builder()
                .httpStatusCode(HttpStatus.ACCEPTED.value())
                .build());
    }

    public ResponseEntity<ItemDeleteResponse> deleteItems(List<String> items) {
        List<ItemKey> itemKeys = items.stream()
                .map(item -> ItemKey.of(item, null))
                .toList();
        return deleteItemsByItemKey(itemKeys);
    }

    private ResponseEntity<ItemDeleteResponse> deleteItemsByItemKey(List<ItemKey> items) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        log.debug("Received DELETE request for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

        int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
        LogPosition offset;
        long now = System.currentTimeMillis();

        try (AutoCloseableLock l = writeOperationLock()) {
            long index = wal.appendDeleteOperation(leaderTerm, now, items);
            offset = LogPosition.of(leaderTerm, index);
            offsetState.setLogTimestamp(offset, now);
        } catch (Exception e) {
            return ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build());
        }

        long timeoutMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long start = System.nanoTime();
        log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);

        CompletableFuture.runAsync(() -> {
            boolean committed = false;

            while (System.nanoTime() - start < timeoutNanos) {
                if (offsetState.getCommittedOffset().isGreaterThanOrEquals(offset)) {
                    try (AutoCloseableLock l2 = writeDataLock()) {
                        for (ItemKey itemKey : items) {
                            data.remove(itemKey);
                            dataExpiryTimes.remove(itemKey);
                        }
                    }

                    log.debug("Successfully committed DELETE operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);
                    committed = true;
                    break;
                }

                // TODO - this shouldn't be hard coded
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                }
            }

            if (!committed) {
                String errorMessage = "DELETE operation for '{}' items to table '{}' partition '{}' was not committed in the expected time.".formatted(
                        items.size(),
                        table,
                        partition);
                log.error(errorMessage);
            }
        });

        return ResponseEntity.ok().body(ItemDeleteResponse.builder()
                .httpStatusCode(HttpStatus.ACCEPTED.value())
                .build());
    }

    public ResponseEntity<ItemGetResponse> getItem(String key) {
        try (AutoCloseableLock l = readDataLock()) {
            if (!data.containsKey(key)) {
                return ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build());
            }
            return ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .value(data.get(key))
                    .build());
        }
    }

    public ResponseEntity<ItemListResponse> listItems(int limit) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        // TODO - 100 shouldn't be hard coded
        try (AutoCloseableLock l = readDataLock()) {
            return ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(data.entrySet()
                            .stream()
                            .map(e -> Map.entry(e.getKey().keyString(), e.getValue()))
                            .limit(Math.min(limit, 100))
                            .toList())
                    .build());
        }
    }

    public void appendLogEntries(List<WALEntry> logEntries,
                                 LogPosition commitedOffset) {
        try (AutoCloseableLock l = writeDataLock()) {
            LogPosition currentCommitedOffset = offsetState.getCommittedOffset();

            if (!logEntries.isEmpty())
                wal.appendLogs(logEntries);

            if (commitedOffset.equals(LogPosition.ZERO) || commitedOffset.equals(currentCommitedOffset))
                return;

            List<WALEntry> logEntriesNotAppliedYet = wal.readLogs(currentCommitedOffset, commitedOffset);
            for (WALEntry walEntry : logEntriesNotAppliedYet) {
                ItemKey itemKey = ItemKey.of(walEntry.key(), walEntry.expiryTime());
                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        data.put(itemKey, walEntry.value());
                        if (walEntry.expiryTime() != null)
                            dataExpiryTimes.offer(itemKey);
                    }
                    case OperationType.DELETE -> {
                        data.remove(itemKey);
                        if (walEntry.expiryTime() != null)
                            dataExpiryTimes.remove(itemKey);
                    }
                }
            }
            offsetState.setCommittedOffset(commitedOffset);
        }
    }

    public void applyDataSnapshot(DataSnapshot dataSnapshot) {
        try (AutoCloseableLock l = writeDataLock()) {
            wal.setEndOffset(dataSnapshot.getLastCommittedOffset());
            offsetState.setCommittedOffset(dataSnapshot.getLastCommittedOffset());

            for (Map.Entry<ItemKey, String> entry : dataSnapshot.getData().entrySet()) {
                data.put(entry.getKey(), entry.getValue());
                if (entry.getKey().expiryTime() != null)
                    dataExpiryTimes.offer(entry.getKey());
            }
        }
    }

    public ResponseEntity<WALFetchResponse> getLogEntries(LogPosition lastFetchOffset,
                                                          int maxNumRecords,
                                                          String replicaId) {
        try {
            log.debug("Received WAL fetch request from replica '{}' for table '{}' partition '{}' offset {}.",
                    replicaId,
                    table,
                    partition,
                    lastFetchOffset);

            offsetState.setReplicaEndOffset(replicaId, lastFetchOffset);

            LogPosition startOffset = wal.getStartOffset();
            LogPosition endOffset = wal.getEndOffset();

            if (endOffset.equals(LogPosition.ZERO)) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(new ArrayList<>())
                        .commitedOffset(LogPosition.ZERO)
                        .payloadType(WALFetchPayloadType.LOG)
                        .build());
            }

            int currentLeaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);

            if (lastFetchOffset.leaderTerm() == currentLeaderTerm - 1) {
                LogPosition previousLeaderEndOffset = offsetState.getPreviousLeaderEndOffset();

                if (lastFetchOffset.isGreaterThan(previousLeaderEndOffset)) {
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.CONFLICT.value())
                            .truncateToOffset(previousLeaderEndOffset)
                            .build());
                }
            }

            LogPosition commitedOffset = offsetState.getCommittedOffset();

            if (lastFetchOffset.equals(endOffset)) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(new ArrayList<>())
                        .commitedOffset(commitedOffset)
                        .payloadType(WALFetchPayloadType.LOG)
                        .build());
            }

            if (startOffset.equals(LogPosition.ZERO) || lastFetchOffset.isLessThan(startOffset)) {
                byte[] dataSnapshotBytes = readDataSnapshotBytes();

                if (dataSnapshotBytes == null) {
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage("No WAL or data snapshot found matching the requested offset range.")
                            .build());
                }

                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .payloadType(WALFetchPayloadType.SNAPSHOT)
                        .dataSnapshotBytes(dataSnapshotBytes)
                        .build());
            }

            List<WALEntry> entries = wal.readLogs(lastFetchOffset, maxNumRecords);

            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .entries(entries)
                    .commitedOffset(commitedOffset)
                    .payloadType(WALFetchPayloadType.LOG)
                    .build());
        } catch (Exception e) {
            return ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build());
        }
    }

    public ResponseEntity<ItemCountAndOffsetsResponse> countItems() {
        try (AutoCloseableLock l2 = readDataLock()) {
            return ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .count(data.size())
                    .commitedOffset(offsetState.getCommittedOffset())
                    .endOffset(wal.getEndOffset())
                    .leaderId(MetadataCache.getInstance().getLeaderNodeId(table, partition))
                    .build());
        }
    }

    public void newLeaderElected(NewLeaderElectedRequest request) {
        try (AutoCloseableLock l = writeDataLock()) {
            if (nodeId.equals(request.getNewLeaderId())) {
                log.info("This node elected as the new leader for table '{}' partition '{}'. Now performing offset synchronization and truncation.",
                        request.getTableName(),
                        request.getPartitionId());
                Set<String> isrEndpoints = MetadataCache.getInstance().getISREndpoints(
                        request.getTableName(),
                        request.getPartitionId(),
                        nodeId);
                WALGetReplicaEndOffsetRequest walGetReplicaEndOffsetRequest = new WALGetReplicaEndOffsetRequest(
                        request.getTableName(),
                        request.getPartitionId());
                LogPosition thisReplicaEndOffset = wal.getEndOffset();
                LogPosition thisReplicaCommittedOffset = offsetState.getCommittedOffset();
                LogPosition earliestISREndOffset = thisReplicaEndOffset;

                for (String isrEndpoint : isrEndpoints) {
                    try {
                        WALGetReplicaEndOffsetResponse response = RestClient.builder()
                                .build()
                                .post()
                                .uri("http://%s/api/wal/get-end-offset/".formatted(isrEndpoint))
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(walGetReplicaEndOffsetRequest)
                                .retrieve()
                                .toEntity(WALGetReplicaEndOffsetResponse.class)
                                .getBody();

                        if (response.getEndOffset().isLessThan(earliestISREndOffset))
                            earliestISREndOffset = response.getEndOffset();
                    } catch (Exception e) {
                        log.warn("Log synchronization request to endpoint '{}' failed.", isrEndpoint, e);
                    }
                }

                if (earliestISREndOffset.isLessThan(thisReplicaEndOffset)) {
                    log.info("Detected uncommitted offsets from the previous leader. Truncating WAL to before offset {}.", earliestISREndOffset);
                    wal.truncateToBeforeInclusive(earliestISREndOffset);
                }

                if (earliestISREndOffset.isGreaterThan(thisReplicaCommittedOffset)) {
                    offsetState.setCommittedOffset(earliestISREndOffset);
                    List<WALEntry> walEntries = wal.readLogs(thisReplicaCommittedOffset, earliestISREndOffset);
                    for (WALEntry walEntry : walEntries) {
                        ItemKey itemKey = ItemKey.of(walEntry.key(), walEntry.expiryTime());

                        switch (walEntry.operation()) {
                            case OperationType.PUT -> {
                                data.put(itemKey, walEntry.value());
                                if (walEntry.expiryTime() != null)
                                    dataExpiryTimes.offer(itemKey);
                            }
                            case OperationType.DELETE -> {
                                data.remove(itemKey);
                                if (walEntry.expiryTime() != null)
                                    dataExpiryTimes.remove(itemKey);
                            }
                        }
                    }
                }

                offsetState.setPreviousLeaderEndOffset(earliestISREndOffset);
            } else {
               offsetState.clearPreviousLeaderEndOffset();
            }
        }
    }

    @Override
    public void close() {
        if (wal != null)
            wal.close();
    }

    public void takeDataSnapshot() {
        try {
            LogPosition committedOffset = offsetState.getCommittedOffset();

            if (committedOffset.equals(LogPosition.ZERO))
                return;

            DataSnapshot lastSnapshot = readDataSnapshot();

            if (lastSnapshot != null && lastSnapshot.getLastCommittedOffset().equals(committedOffset)) {
                log.debug("Skipping taking data snapshot for table '{}' partition '{}'. " +
                          "Last snapshot committed offset is '{}', committed offset is '{}'.",
                        table,
                        partition,
                        lastSnapshot.getLastCommittedOffset(),
                        committedOffset);
                return;
            }

            HashMap<ItemKey, String> dataCopy;
            try (AutoCloseableLock l = readDataLock()) {
                dataCopy = new HashMap<ItemKey, String>(data);
            }
            DataSnapshot snapshot = new DataSnapshot(committedOffset, dataCopy);

            CompressionUtil.compressAndWrite(dataSnapshotFile, snapshot);
            ChecksumUtil.generateAndWrite(dataSnapshotFile);
            wal.truncateToAfterExclusive(committedOffset);
            log.debug("Took data snapshot at last applied offset '{}' for table '{}' partition '{}'. The data size is: {}",
                    committedOffset,
                    table,
                    partition,
                    dataCopy.size());
        } catch (Exception e) {
            String errorMessage = "Error taking a snapshot of data for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
        }
    }

    public void flushLeaderWAL() {
        if (nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition)))
            wal.flush();
    }

    private void createPartitionDirectoryIfNotExists() {
        Path partitionDir = Path.of("%s/%s-%s".formatted(storageSettings.getNode().getStorageDir(), table, partition));
        try {
            if (Files.notExists(partitionDir)) {
                Files.createDirectory(partitionDir);
                log.debug("Partition directory '%s' created.".formatted(partitionDir));
            }
        } catch (Exception e) {
            String errorMessage = "Error creating partition directory '%s'.".formatted(partitionDir);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private DataSnapshot readDataSnapshot() {
        if (!Files.exists(Path.of(dataSnapshotFile)))
            return null;

        try {
            ChecksumUtil.readAndVerify(dataSnapshotFile);
            return CompressionUtil.readAndDecompress(dataSnapshotFile, DataSnapshot.class);
        } catch (Exception e) {
            String errorMessage = "Error reading data snapshot file for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private byte[] readDataSnapshotBytes() {
        if (!Files.exists(Path.of(dataSnapshotFile)))
            return null;

        try {
            ChecksumUtil.readAndVerify(dataSnapshotFile);
            return CompressionUtil.readAndDecompress(dataSnapshotFile);
        } catch (Exception e) {
            String errorMessage = "Error reading data snapshot file for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
            throw new StorageServerException(errorMessage, e);
        }
    }

    private void loadFromDataSnapshotAndWALFile() {
        try (AutoCloseableLock l = writeDataLock()) {
            if (Files.exists(Path.of(dataSnapshotFile))) {
                DataSnapshot dataSnapshot = readDataSnapshot();
                if (dataSnapshot != null) {
                    for (Map.Entry<ItemKey, String> entry : dataSnapshot.getData().entrySet()) {
                        // TODO - do we need to recover log timestamps from snapshot?
                        data.put(entry.getKey(), entry.getValue());
                        if (entry.getKey().expiryTime() != null)
                            dataExpiryTimes.offer(entry.getKey());
                    }
                    wal.setEndOffset(dataSnapshot.getLastCommittedOffset());
                    log.debug("Loaded '{}' data items from a snapshot at offset '{}' for table '{}' partition '{}' .",
                            dataSnapshot.getData().size(),
                            dataSnapshot.getLastCommittedOffset(),
                            table,
                            partition);
                }
            }

            List<WALEntry> logEntriesFromFile = wal.loadFromFile();
            for (WALEntry walEntry : logEntriesFromFile) {
                ItemKey itemKey = ItemKey.of(walEntry.key(), walEntry.expiryTime());
                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        data.put(itemKey, walEntry.value());
                        if (walEntry.expiryTime() != null)
                            dataExpiryTimes.offer(itemKey);
                    }
                    case OperationType.DELETE -> {
                        data.remove(itemKey);
                        if (walEntry.expiryTime() != null)
                            dataExpiryTimes.remove(itemKey);
                    }
                }
            }
            log.debug("Loaded '{}' log entries from WAL file for table '{}' partition '{}'.",
                      logEntriesFromFile.size(),
                      table,
                      partition);
        }
    }

    private AutoCloseableLock readDataLock() {
        return AutoCloseableLock.acquire(dataLock.readLock());
    }

    private AutoCloseableLock writeDataLock() {
        return AutoCloseableLock.acquire(dataLock.writeLock());
    }

    private AutoCloseableLock writeOperationLock() {
        return AutoCloseableLock.acquire(operationLock.writeLock());
    }
}
