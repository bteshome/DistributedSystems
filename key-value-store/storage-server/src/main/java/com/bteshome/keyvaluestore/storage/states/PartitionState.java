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
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
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
    private final WAL wal;
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
        loadFromDataSnapshotAndWALFile();
        offsetState = new OffsetState(table, partition, storageSettings);
    }

    public WAL getWal() {
        try (AutoCloseableLock l = readDataLock()) {
            return wal;
        }
    }

    public OffsetState getOffsetState() {
        try (AutoCloseableLock l = readDataLock()) {
            return offsetState;
        }
    }

    public void expireItems() {
        List<ItemKey> itemsToRemove = new ArrayList<>();

        try (AutoCloseableLock l = writeDataLock()) {
            Instant now = Instant.now();
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

    public ResponseEntity<ItemPutResponse> putItems(List<Item> items) {
        if (!nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition))) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build());
        }

        log.debug("Received PUT request for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

        try (AutoCloseableLock l = writeOperationLock()) {
            int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
            LogPosition offset;
            Instant expiryTime = (timeToLive == null || timeToLive.equals(Duration.ZERO)) ? null : Instant.now().plus(timeToLive);

            try (AutoCloseableLock l2 = writeDataLock()) {
                long index = wal.appendPutOperation(leaderTerm, items, expiryTime);
                offset = LogPosition.of(leaderTerm, index);
                offsetState.setEndOffset(offset);
            } catch (Exception e) {
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }

            long timeoutMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            long start = System.nanoTime();
            log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);
            Set<String> currentISRNodeIds = Set.of();
            Set<Replica> newISRs = Set.of();
            Set<Replica> laggingCurrentISRs = Set.of();

            while (System.nanoTime() - start < timeoutNanos) {
                Tuple4<Boolean, Set<String>, Set<Replica>, Set<Replica>> acknowledgementCheckResult = isFullyAcknowledged(offset);
                boolean canBeCommitted = acknowledgementCheckResult.first();
                currentISRNodeIds = acknowledgementCheckResult.second();
                newISRs = acknowledgementCheckResult.third();
                laggingCurrentISRs = acknowledgementCheckResult.fourth();

                if (canBeCommitted) {
                    try (AutoCloseableLock l2 = writeDataLock()) {
                        offsetState.setCommittedOffset(offset);
                        for (Item item : items) {
                            ItemKey itemKey = ItemKey.of(item.getKey(), expiryTime);
                            data.put(itemKey, item.getValue());
                            if (expiryTime != null)
                                dataExpiryTimes.offer(itemKey);
                        }
                    } catch (StorageServerException e) {
                        return ResponseEntity.ok(ItemPutResponse.builder()
                                .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                                .errorMessage(e.getMessage())
                                .build());
                    }

                    sendNewISRsRequest(currentISRNodeIds, newISRs);
                    log.debug("Successfully committed PUT operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.OK.value())
                            .build());
                }

                // TODO - this shouldn't be hard coded
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                    return ResponseEntity.ok(ItemPutResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
                }
            }

            sendLaggingISRsRequest(currentISRNodeIds, laggingCurrentISRs);

            String errorMessage = "Request timed out.";
            log.error(errorMessage);
            return ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.REQUEST_TIMEOUT.value())
                    .errorMessage(errorMessage)
                    .build());
        }
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

        try (AutoCloseableLock l = writeOperationLock()) {
            int leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
            LogPosition offset;

            try (AutoCloseableLock l2 = writeDataLock()) {
                long index = wal.appendDeleteOperation(leaderTerm, items);
                offset = LogPosition.of(leaderTerm, index);
                offsetState.setEndOffset(offset);
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
            Set<String> currentISRNodeIds = null;
            Set<Replica> newISRs = Set.of();
            Set<Replica> laggingCurrentISRs = Set.of();

            while (System.nanoTime() - start < timeoutNanos) {
                Tuple4<Boolean, Set<String>, Set<Replica>, Set<Replica>> acknowledgementCheckResult = isFullyAcknowledged(offset);
                boolean canBeCommitted = acknowledgementCheckResult.first();
                currentISRNodeIds = acknowledgementCheckResult.second();
                newISRs = acknowledgementCheckResult.third();
                laggingCurrentISRs = acknowledgementCheckResult.fourth();

                if (canBeCommitted) {
                    try (AutoCloseableLock l2 = writeDataLock()) {
                        offsetState.setCommittedOffset(offset);
                        for (ItemKey itemKey : items) {
                            data.remove(itemKey);
                            dataExpiryTimes.remove(itemKey);
                        }
                    } catch (StorageServerException e) {
                        return ResponseEntity.ok(ItemDeleteResponse.builder()
                                .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                                .errorMessage(e.getMessage())
                                .build());
                    }

                    sendNewISRsRequest(currentISRNodeIds, newISRs);
                    log.debug("Successfully committed DELETE operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);

                    return ResponseEntity.ok(ItemDeleteResponse.builder()
                            .httpStatusCode(HttpStatus.OK.value())
                            .build());
                }

                // TODO - this shouldn't be hard coded
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    String errorMessage = "Error waiting for replicas to acknowledge the log entry.";
                    log.error(errorMessage, e);
                    return ResponseEntity.ok(ItemDeleteResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
                }
            }

            sendLaggingISRsRequest(currentISRNodeIds, laggingCurrentISRs);

            String errorMessage = "Request timed out.";
            log.error(errorMessage);
            return ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.REQUEST_TIMEOUT.value())
                    .errorMessage(errorMessage)
                    .build());
        }
    }

    private void sendNewISRsRequest(Set<String> currentISRNodeIds, final Set<Replica> newISRs) {
        if (!newISRs.isEmpty()) {
            log.info("Current ISRs for table '{}' partition '{}' are: {}. News ISRs are {}.",
                    table,
                    partition,
                    currentISRNodeIds,
                    newISRs.stream().map(Replica::getNodeId));

            CompletableFuture.runAsync(() -> isrSynchronizer.addToInSyncReplicaLists(newISRs));
            log.debug("Sent new ISRs request for table '{}' partition '{}'.",
                    table,
                    partition);
        }
    }

    private void sendLaggingISRsRequest(Set<String> currentISRNodeIds, final Set<Replica> laggingCurrentISRs) {
        if (!laggingCurrentISRs.isEmpty()) {
            log.info("Current ISRs for table '{}' partition '{}' are: {}. Lagging ISRs are: {}",
                    table,
                    partition,
                    currentISRNodeIds,
                    laggingCurrentISRs.stream().map(Replica::getNodeId));

            CompletableFuture.runAsync(() -> isrSynchronizer.removeFromInSyncReplicaLists(laggingCurrentISRs));
            log.debug("Sent lagging ISRs request for table '{}' partition '{}'.",
                    table,
                    partition);
        }
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
            if (!logEntries.isEmpty()) {
                LogPosition endOffset = wal.appendLogs(logEntries);
                offsetState.setEndOffset(endOffset);
            }
            if (!commitedOffset.equals(currentCommitedOffset)) {
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
    }

    public void applyDataSnapshot(DataSnapshot dataSnapshot) {
        try (AutoCloseableLock l = writeDataLock()) {
            offsetState.setEndOffset(dataSnapshot.getLastCommittedOffset());
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
        try (AutoCloseableLock l2 = readDataLock()) {
            LogPosition thisReplicaEndOffset = offsetState.getEndOffset();

            if (thisReplicaEndOffset.equals(LogPosition.empty())) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(List.of())
                        .commitedOffset(LogPosition.empty())
                        .payloadType(WALFetchPayloadType.LOG)
                        .build());
            }

            int currentLeaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);

            if (lastFetchOffset.leaderTerm() == currentLeaderTerm - 1) {
                LogPosition previousLeaderEndOffset = offsetState.getPreviousLeaderEndOffset();
                if (previousLeaderEndOffset.leaderTerm() != currentLeaderTerm - 1) {
                    String errorMessage = "Previous leader end offset '%s' is not from the previous leader.".formatted(previousLeaderEndOffset);
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage(errorMessage)
                            .build());
                }

                if (lastFetchOffset.isGreaterThan(previousLeaderEndOffset)) {
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.CONFLICT.value())
                            .truncateToOffset(previousLeaderEndOffset)
                            .build());
                }
            }

            LogPosition commitedOffset = offsetState.getCommittedOffset();

            if (lastFetchOffset.equals(thisReplicaEndOffset)) {
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(List.of())
                        .commitedOffset(commitedOffset)
                        .payloadType(WALFetchPayloadType.LOG)
                        .build());
            }

            LogPosition walStartOffset = wal.getStartOffset();
            LogPosition walEndOffset = wal.getEndOffset();

            if (walEndOffset.equals(LogPosition.empty()) || lastFetchOffset.isLessThan(walStartOffset)) {
                DataSnapshot dataSnapshot = readDataSnapshot();
                if (dataSnapshot == null) {
                    return ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .errorMessage("No data snapshot found.")
                            .build());
                }
                return ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .payloadType(WALFetchPayloadType.SNAPSHOT)
                        .dataSnapshot(dataSnapshot)
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
                    .endOffset(offsetState.getEndOffset())
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
                LogPosition thisReplicaEndOffset = offsetState.getEndOffset();
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
                    log.info("Detected uncommitted offsets from the previous leader. Truncating WAL to offset {}.", earliestISREndOffset);
                    wal.truncateToBeforeInclusive(earliestISREndOffset);
                    offsetState.setEndOffset(earliestISREndOffset);
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
        if (wal != null) {
            wal.close();
        }
    }

    public void takeDataSnapshot() {
        try (AutoCloseableLock l = writeDataLock()) {
            DataSnapshot lastSnapshot = readDataSnapshot();
            LogPosition committedOffset = offsetState.getCommittedOffset();

            if (lastSnapshot != null && lastSnapshot.getLastCommittedOffset().equals(committedOffset)) {
                log.debug("Skipping taking data snapshot for table '{}' partition '{}'. " +
                          "Last snapshot committed offset is '{}', committed offset is '{}'.",
                        table,
                        partition,
                        lastSnapshot.getLastCommittedOffset(),
                        committedOffset);
            } else {
                DataSnapshot snapshot = new DataSnapshot();
                snapshot.setData(data);
                snapshot.setLastCommittedOffset(committedOffset);
                CompressionUtil.compressAndWrite(dataSnapshotFile, snapshot);
                ChecksumUtil.generateAndWrite(dataSnapshotFile);
                wal.truncateToAfterExclusive(committedOffset);
                log.debug("Took data snapshot at last applied offset '{}' for table '{}' partition '{}'. The data size is: {}",
                        committedOffset,
                        table,
                        partition,
                        data.size());
            }
        } catch (Exception e) {
            String errorMessage = "Error taking a snapshot of data for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
        }
    }

    private Tuple4<Boolean, Set<String>, Set<Replica>, Set<Replica>> isFullyAcknowledged(LogPosition offset) {
        int numReplicasThatAcknowledged = 1;
        int minISRCount = MetadataCache.getInstance().getMinInSyncReplicas(table);
        Set<String> currentISRNodeIds = MetadataCache.getInstance().getInSyncReplicas(table, partition);
        Set<Replica> currentISRsThatDidNotAcknowledge = new HashSet<>();
        Set<Replica> newISRs = new HashSet<>();

        if (minISRCount <= 1)
            return Tuple4.of(true, currentISRNodeIds, newISRs, currentISRsThatDidNotAcknowledge);

        Set<String> replicaNodeIds = MetadataCache.getInstance().getReplicaNodeIds(
                table,
                partition);
        WALGetReplicaEndOffsetRequest walGetReplicaEndOffsetRequest = new WALGetReplicaEndOffsetRequest(
                table,
                partition);

        for (String nodeId : replicaNodeIds) {
            if (nodeId.equals(this.nodeId))
                continue;

            String endpoint = MetadataCache.getInstance().getEndpoint(nodeId);

            try {
                // TODO - 1. what should these numbers be? 2. should they be configurable?
                HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
                factory.setConnectTimeout(1000);
                factory.setConnectionRequestTimeout(1000);
                factory.setReadTimeout(1000);
                WALGetReplicaEndOffsetResponse response = RestClient.builder()
                        .requestFactory(factory)
                        .build()
                        .post()
                        .uri("http://%s/api/wal/get-end-offset/".formatted(endpoint))
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(walGetReplicaEndOffsetRequest)
                        .retrieve()
                        .toEntity(WALGetReplicaEndOffsetResponse.class)
                        .getBody();

                if (response.getEndOffset().isGreaterThanOrEquals(offset)) {
                    numReplicasThatAcknowledged++;
                    if (!currentISRNodeIds.contains(nodeId))
                        newISRs.add(new Replica(nodeId, table, partition));
                } else {
                    if (currentISRNodeIds.contains(nodeId))
                        currentISRsThatDidNotAcknowledge.add(new Replica(nodeId, table, partition));
                }
            } catch (Exception e) {
                log.warn("Error getting replica end offset from endpoint '{}' for table '{}' partition '{}'.",
                        endpoint,
                        table,
                        partition,
                        e);
            }
        }

        boolean isAcknowledged = numReplicasThatAcknowledged >= minISRCount;
        return Tuple4.of(isAcknowledged, currentISRNodeIds, newISRs, currentISRsThatDidNotAcknowledge);
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

    private void loadFromDataSnapshotAndWALFile() {
        try (AutoCloseableLock l = writeDataLock()) {
            if (Files.exists(Path.of(dataSnapshotFile))) {
                DataSnapshot dataSnapshot = readDataSnapshot();
                if (dataSnapshot != null) {
                    for (Map.Entry<ItemKey, String> entry : dataSnapshot.getData().entrySet()) {
                        data.put(entry.getKey(), entry.getValue());
                        if (entry.getKey().expiryTime() != null)
                            dataExpiryTimes.offer(entry.getKey());
                    }
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
