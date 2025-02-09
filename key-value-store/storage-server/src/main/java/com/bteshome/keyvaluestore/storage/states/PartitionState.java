package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.*;
import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Item;
import com.bteshome.keyvaluestore.common.requests.ISRListChangedRequest;
import com.bteshome.keyvaluestore.common.requests.NewLeaderElectedRequest;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.CompressionUtil;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.core.ReplicationMonitor;
import com.bteshome.keyvaluestore.storage.core.WALFetcher;
import com.bteshome.keyvaluestore.storage.entities.*;
import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchPayloadType;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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
    private final Sinks.Many<LogPosition> replicationMonitorSink;
    @Getter
    private final WAL wal;
    @Getter
    private final OffsetState offsetState;
    private final ReentrantReadWriteLock leaderLock;
    private final ReentrantReadWriteLock replicaLock;
    private final StorageSettings storageSettings;
    private final ISRSynchronizer isrSynchronizer;
    private final WebClient webClient;
    private final String dataSnapshotFile;
    private final long writeTimeoutMs;
    private final long timeLagThresholdMs;
    private final long recordLagThreshold;
    private final int fetchMaxNumRecords;
    private final int minISRCount;
    private final Set<String> allReplicaIds;
    private String leaderEndpoint;
    private boolean isLeader;
    private int leaderTerm;
    private Set<String> inSyncReplicas;

    public PartitionState(String table,
                          int partition,
                          StorageSettings storageSettings,
                          ISRSynchronizer isrSynchronizer,
                          WebClient webClient) {
        log.info("Creating partition state for table '{}' partition '{}' on replica '{}'.", table, partition, storageSettings.getNode().getId());
        this.table = table;
        this.partition = partition;
        this.timeToLive = MetadataCache.getInstance().getTableTimeToLive(table, partition);
        this.nodeId = storageSettings.getNode().getId();
        this.writeTimeoutMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.WRITE_TIMEOUT_MS_KEY);
        this.timeLagThresholdMs = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_TIME_MS_KEY);
        this.recordLagThreshold = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_LAG_THRESHOLD_RECORDS_KEY);
        this.fetchMaxNumRecords = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.REPLICA_FETCH_MAX_NUM_RECORDS_KEY);
        this.minISRCount = MetadataCache.getInstance().getMinInSyncReplicas(table);
        this.allReplicaIds = MetadataCache.getInstance().getReplicaNodeIds(table, partition);
        this.isLeader = this.nodeId.equals(MetadataCache.getInstance().getLeaderNodeId(table, partition));
        this.leaderTerm = MetadataCache.getInstance().getLeaderTerm(table, partition);
        this.leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
        if (isLeader)
            this.inSyncReplicas = MetadataCache.getInstance().getInSyncReplicas(table, partition);
        this.storageSettings = storageSettings;
        this.isrSynchronizer = isrSynchronizer;
        this.webClient = webClient;
        this.leaderLock = new ReentrantReadWriteLock(true);
        this.replicaLock = new ReentrantReadWriteLock(true);
        this.data = new ConcurrentHashMap<>();
        this.dataExpiryTimes = new PriorityQueue<>(Comparator.comparing(ItemKey::expiryTime));
        this.replicationMonitorSink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
        createPartitionDirectoryIfNotExists();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        dataSnapshotFile = "%s/%s-%s/data.ser.snappy".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        offsetState = new OffsetState(table, partition, storageSettings);
        loadFromDataSnapshotAndWALFile();
    }

    /**
     * The code in the following section applies to the leader only.
     * */
    public void checkReplicaStatus() {
        if (!isLeader)
            return;

        try (AutoCloseableLock l = writeLeaderLock()) {
            LogPosition committedOffset = offsetState.getCommittedOffset();
            LogPosition newCommittedOffset = ReplicationMonitor.check(this,
                    nodeId,
                    table,
                    partition,
                    minISRCount,
                    allReplicaIds,
                    inSyncReplicas,
                    timeLagThresholdMs,
                    recordLagThreshold,
                    isrSynchronizer);

            if (newCommittedOffset.isGreaterThan(committedOffset)) {
                List<WALEntry> walEntriesNotCommittedYet = wal.readLogs(committedOffset, newCommittedOffset);

                log.debug("Replication monitor for table '{}' partition '{}' found a new offset eligible for commit. " +
                        "Current committed offset is '{}', new offset is '{}'. # of items is: '{}'.",
                        table,
                        partition,
                        committedOffset,
                        newCommittedOffset,
                        walEntriesNotCommittedYet.size());

                for (WALEntry walEntry : walEntriesNotCommittedYet) {
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

                offsetState.setCommittedOffset(newCommittedOffset);
                replicationMonitorSink.tryEmitNext(newCommittedOffset);
            }
        } catch (Exception e) {
            log.error("Error checking replica status for table '{}' partition '{}'.", table, partition, e);
        }
    }

    public Mono<ResponseEntity<ItemPutResponse>> putItems(final ItemPutRequest request) {
        if (!isLeader) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return Mono.just(ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build()));
        }

        log.debug("Received PUT request for '{}' items to table '{}' partition '{}' with ack type '{}'.",
                request.getItems().size(),
                table,
                partition,
                request.getAck());

        Mono<ResponseEntity<ItemPutResponse>> mono = Mono.fromCallable(() -> {
            try (AutoCloseableLock l = writeLeaderLock()) {
                Instant expiryTime = (timeToLive == null || timeToLive.equals(Duration.ZERO)) ? null : Instant.now().plus(timeToLive);
                long now = System.currentTimeMillis();
                long index = wal.appendPutOperation(leaderTerm, now, request.getItems(), expiryTime);
                LogPosition offset = LogPosition.of(leaderTerm, index);
                offsetState.setEndOffset(offset);
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .endOffset(offset)
                        .build());
            } catch (Exception e) {
                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }
        }).subscribeOn(Schedulers.boundedElastic());

        if (request.getAck() == null || request.getAck().equals(AckType.NONE)) {
            mono.subscribe();
            return Mono.just(ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .build()));
        }

        if (request.getAck().equals(AckType.LEADER) || minISRCount == 1)
            return mono;

        return mono.flatMap(walAppendResponseEntity -> {
            final ItemPutResponse response = walAppendResponseEntity.getBody();
            final LogPosition offset = response.getEndOffset();
            final List<Item> items = request.getItems();

            if (response.getHttpStatusCode() != HttpStatus.OK.value())
                return Mono.just(walAppendResponseEntity);

            log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);

            return Mono.defer(() -> replicationMonitorSink.asFlux()
                    .takeUntil(c -> c.isGreaterThanOrEquals(offset))
                    .takeUntilOther(Mono.delay(Duration.ofMillis(writeTimeoutMs)))
                    .last(LogPosition.ZERO)
                    .doOnNext(c -> {
                        if (c.isGreaterThanOrEquals(offset)) {
                            log.debug("Successfully committed PUT operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);
                            response.setHttpStatusCode(HttpStatus.OK.value());
                        }
                        else {
                            String errorMessage = "PUT operation for '%s' items to table '%s' partition '%s' timed out.".formatted(items.size(), table, partition);
                            log.debug(errorMessage);
                            response.setHttpStatusCode(HttpStatus.REQUEST_TIMEOUT.value());
                        }
                    })
                    .map(c -> ResponseEntity.ok(response)));
        });
    }

    public Mono<ResponseEntity<ItemDeleteResponse>> deleteItems(ItemDeleteRequest request) {
        if (!isLeader) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return Mono.just(ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build()));
        }

        log.debug("Received DELETE request for '{}' items to table '{}' partition '{}' with ack type '{}'.",
                request.getKeys().size(),
                table,
                partition,
                request.getAck());

        List<ItemKey> itemKeys = request.getKeys().stream()
                .map(item -> ItemKey.of(item, null))
                .toList();

        return deleteItemsByItemKey(itemKeys, request.getAck());
    }

    public void deleteExpiredItems() {
        if (!isLeader)
            return;

        log.debug("Item expiration monitor about to check if any items have expired.");

        List<ItemKey> itemsToRemove = new ArrayList<>();
        Instant now = Instant.now();

        try (AutoCloseableLock l = writeLeaderLock()) {
            while (!dataExpiryTimes.isEmpty() &&
                    (dataExpiryTimes.peek().expiryTime().isBefore(now) ||
                            dataExpiryTimes.peek().expiryTime().equals(now))) {
                ItemKey itemKey = dataExpiryTimes.poll();
                itemsToRemove.add(itemKey);
            }
        }

        if (!itemsToRemove.isEmpty()) {
            log.debug("Expiring '{}' items.", itemsToRemove.size());
            deleteItemsByItemKey(itemsToRemove, null);
        }
    }

    private Mono<ResponseEntity<ItemDeleteResponse>> deleteItemsByItemKey(final List<ItemKey> items, AckType ack) {
        Mono<ResponseEntity<ItemDeleteResponse>> mono = Mono.fromCallable(() -> {
            try (AutoCloseableLock l = writeLeaderLock()) {
                long now = System.currentTimeMillis();
                long index = wal.appendDeleteOperation(leaderTerm, now, items);
                LogPosition offset = LogPosition.of(leaderTerm, index);
                offsetState.setEndOffset(offset);
                return ResponseEntity.ok(ItemDeleteResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .endOffset(offset)
                        .build());
            } catch (Exception e) {
                return ResponseEntity.ok(ItemDeleteResponse.builder()
                        .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                        .errorMessage(e.getMessage())
                        .build());
            }
        }).subscribeOn(Schedulers.boundedElastic());

        if (ack == null || ack.equals(AckType.NONE)) {
            mono.subscribe();
            return Mono.just(ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .build()));
        }

        if (ack.equals(AckType.LEADER) || minISRCount == 1)
            return mono;

        return mono.flatMap(walAppendResponseEntity -> {
            final ItemDeleteResponse response = walAppendResponseEntity.getBody();
            final LogPosition offset = response.getEndOffset();

            if (response.getHttpStatusCode() != HttpStatus.OK.value())
                return Mono.just(walAppendResponseEntity);

            log.debug("Waiting for sufficient replicas to acknowledge the log entry at offset {}.", offset);

            return Mono.defer(() -> replicationMonitorSink.asFlux()
                    .takeUntil(c -> c.isGreaterThanOrEquals(offset))
                    .takeUntilOther(Mono.delay(Duration.ofMillis(writeTimeoutMs)))
                    .last(LogPosition.ZERO)
                    .doOnNext(c -> {
                        if (c.isGreaterThanOrEquals(offset)) {
                            log.debug("Successfully committed DELETE operation for '{}' items to table '{}' partition '{}'.", items.size(), table, partition);
                            response.setHttpStatusCode(HttpStatus.OK.value());
                        }
                        else {
                            String errorMessage = "DELETE operation for '%s' items to table '%s' partition '%s' timed out.".formatted(items.size(), table, partition);
                            log.debug(errorMessage);
                            response.setHttpStatusCode(HttpStatus.REQUEST_TIMEOUT.value());
                        }
                    })
                    .map(c -> ResponseEntity.ok(response)));
        });
    }

    public Mono<ResponseEntity<ItemGetResponse>> getItem(String keyString) {
        if (!isLeader) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build()));
        }

        ItemKey key = ItemKey.of(keyString, null);

        if (!data.containsKey(key)) {
            return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .value(data.get(key))
                .build()));
    }

    public Mono<ResponseEntity<ItemListResponse>> listItems(int limit) {
        if (!isLeader) {
            String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .leaderEndpoint(leaderEndpoint)
                    .build()));
        }

        return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .items(data.entrySet()
                        .stream()
                        .map(e -> Map.entry(e.getKey().keyString(), e.getValue()))
                        .limit(limit)
                        .toList())
                .build()));
    }

    public Mono<ResponseEntity<WALFetchResponse>> getLogEntries(LogPosition lastFetchOffset,
                                                                int maxNumRecords,
                                                                String replicaId) {
        if (!isLeader) {
            return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .build()));
        }

        offsetState.setReplicaEndOffset(replicaId, lastFetchOffset);

        try (AutoCloseableLock l = readLeaderLock()) {
            LogPosition walStartOffset = wal.getStartOffset();
            LogPosition endOffset = offsetState.getEndOffset();
            LogPosition commitedOffset = offsetState.getCommittedOffset();
            LogPosition previousLeaderEndOffset = offsetState.getPreviousLeaderEndOffset();
            maxNumRecords = Math.min(maxNumRecords, this.fetchMaxNumRecords);

            log.trace("Received WAL fetch request from replica '{}' for table '{}' partition '{}'. " +
                            "Last fetched offset = {}, leader end offset = {}, leader committed offset = {}.",
                    replicaId,
                    table,
                    partition,
                    lastFetchOffset,
                    endOffset,
                    commitedOffset);

            if (endOffset.equals(LogPosition.ZERO)) {
                return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(new ArrayList<>())
                        .commitedOffset(LogPosition.ZERO)
                        .payloadType(WALFetchPayloadType.LOG)
                        .build()));
            }

            if (lastFetchOffset.equals(endOffset)) {
                return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .entries(new ArrayList<>())
                        .commitedOffset(commitedOffset)
                        .payloadType(WALFetchPayloadType.LOG)
                        .build()));
            }

            if (lastFetchOffset.equals(LogPosition.ZERO)) {
                if (walStartOffset.equals(LogPosition.ZERO))
                    return getSnapshot(lastFetchOffset, walStartOffset, endOffset);
                return getLogs(lastFetchOffset, maxNumRecords, commitedOffset);
            } else if (lastFetchOffset.leaderTerm() == leaderTerm) {
                if (walStartOffset.equals(LogPosition.ZERO) || lastFetchOffset.index() < walStartOffset.index() - 1)
                    return getSnapshot(lastFetchOffset, walStartOffset, endOffset);
                return getLogs(lastFetchOffset, maxNumRecords, commitedOffset);
            } else if (lastFetchOffset.leaderTerm() == leaderTerm - 1) {
                if (lastFetchOffset.isGreaterThan(previousLeaderEndOffset)) {
                    return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                            .httpStatusCode(HttpStatus.CONFLICT.value())
                            .truncateToOffset(previousLeaderEndOffset)
                            .build()));
                }
                if (walStartOffset.equals(LogPosition.ZERO) || walStartOffset.leaderTerm() > lastFetchOffset.leaderTerm())
                    return getSnapshot(lastFetchOffset, walStartOffset, endOffset);
                return getLogs(lastFetchOffset, maxNumRecords, commitedOffset);
            } else {
                log.warn("Replica '{}' requested log entries with a last fetch leader term {} but current leader term is {}.",
                        replicaId,
                        lastFetchOffset.leaderTerm(),
                        leaderTerm);
                if (walStartOffset.equals(LogPosition.ZERO) || walStartOffset.leaderTerm() > lastFetchOffset.leaderTerm())
                    return getSnapshot(lastFetchOffset, walStartOffset, endOffset);
                return getLogs(lastFetchOffset, maxNumRecords, commitedOffset);
            }
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
    }

    private Mono<ResponseEntity<WALFetchResponse>> getLogs(LogPosition lastFetchOffset, int maxNumRecords, LogPosition commitedOffset) {
        List<WALEntry> entries = wal.readLogs(lastFetchOffset, maxNumRecords);

        return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .entries(entries)
                .commitedOffset(commitedOffset)
                .payloadType(WALFetchPayloadType.LOG)
                .build()));
    }

    private Mono<ResponseEntity<WALFetchResponse>> getSnapshot(LogPosition lastFetchOffset, LogPosition walStartOffset, LogPosition endOffset) {
        byte[] dataSnapshotBytes = readDataSnapshotBytes();

        if (dataSnapshotBytes == null) {
            String errorMessage = "No WAL or data snapshot found after the requested offset %s. Current wal start offset is %s: and leader end offset is: %s".formatted(
                    lastFetchOffset,
                    walStartOffset,
                    endOffset);
            return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(errorMessage)
                    .build()));
        }

        return Mono.just(ResponseEntity.ok(WALFetchResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .payloadType(WALFetchPayloadType.SNAPSHOT)
                .dataSnapshotBytes(dataSnapshotBytes)
                .build()));
    }

    public void isrListChanged(ISRListChangedRequest request) {
        if (isLeader)
            this.inSyncReplicas = request.getInSyncReplicas();
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

    /**
     * The code in the following section applies to replicas only.
     * */
    public void fetchWal() {
        if (isLeader)
            return;

        try (AutoCloseableLock l = writeReplicaLock()) {
            WALFetcher.fetch(this,
                    nodeId,
                    table,
                    partition,
                    leaderEndpoint,
                    fetchMaxNumRecords,
                    webClient);
        }
    }

    public void appendLogEntries(List<WALEntry> logEntries, LogPosition commitedOffset) {
        LogPosition currentCommitedOffset = offsetState.getCommittedOffset();

        if (!logEntries.isEmpty()) {
            wal.appendLogs(logEntries);
            offsetState.setEndOffset(logEntries.getLast().getPosition());
        }

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

    public void applyDataSnapshot(DataSnapshot dataSnapshot) {
        offsetState.setEndOffset(dataSnapshot.getLastCommittedOffset());
        offsetState.setCommittedOffset(dataSnapshot.getLastCommittedOffset());

        for (Map.Entry<ItemKey, String> entry : dataSnapshot.getData().entrySet()) {
            data.put(entry.getKey(), entry.getValue());
            if (entry.getKey().expiryTime() != null)
                dataExpiryTimes.offer(entry.getKey());
        }
    }

    /**
     * The code below this point applies to both leaders and replicas.
     * */
    public Mono<ResponseEntity<ItemCountAndOffsetsResponse>> countItems() {
        return Mono.just(ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                .httpStatusCode(HttpStatus.OK.value())
                .count(data.size())
                .commitedOffset(offsetState.getCommittedOffset())
                .endOffset(offsetState.getEndOffset())
                .build()));
    }

    public void newLeaderElected(NewLeaderElectedRequest request) {
        try (AutoCloseableLock l = writeLeaderLock();
             AutoCloseableLock l2 = writeReplicaLock()) {
            if (nodeId.equals(request.getNewLeaderId())) {
                this.isLeader = true;
                this.leaderTerm = request.getNewLeaderTerm();
                this.leaderEndpoint = null;
                this.inSyncReplicas = request.getInSyncReplicas();

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
                        Mono<ResponseEntity<WALGetReplicaEndOffsetResponse>> mono = WebClient
                                .create("http://%s/api/wal/get-end-offset/".formatted(isrEndpoint))
                                .post()
                                .contentType(MediaType.APPLICATION_JSON)
                                .accept(MediaType.APPLICATION_JSON)
                                .bodyValue(walGetReplicaEndOffsetRequest)
                                .retrieve()
                                .toEntity(WALGetReplicaEndOffsetResponse.class);

                        WALGetReplicaEndOffsetResponse response = mono.map(HttpEntity::getBody).block();

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
                this.isLeader = false;
                this.leaderTerm = request.getNewLeaderTerm();
                this.leaderEndpoint = MetadataCache.getInstance().getEndpoint(request.getNewLeaderId());
                this.inSyncReplicas = null;
                offsetState.clearPreviousLeaderEndOffset();
            }
        } catch (Exception e) {
            String errorMessage = "Error handling new leader notification for table '%s' partition '%s'.".formatted(table, partition);
            log.error(errorMessage, e);
        }
    }

    @Override
    public void close() {
        if (wal != null)
            wal.close();
    }

    public void takeDataSnapshot() {
        try (AutoCloseableLock l = writeLeaderLock();
             AutoCloseableLock l2 = writeReplicaLock();) {
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

            HashMap<ItemKey, String> dataCopy = new HashMap<>(data);
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
        try (AutoCloseableLock l = writeLeaderLock();
             AutoCloseableLock l2 = writeReplicaLock()) {
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
                    offsetState.setEndOffset(dataSnapshot.getLastCommittedOffset());
                    log.debug("Loaded '{}' data items from a snapshot at offset '{}' for table '{}' partition '{}' .",
                            dataSnapshot.getData().size(),
                            dataSnapshot.getLastCommittedOffset(),
                            table,
                            partition);
                }
            }

            List<WALEntry> logEntriesFromFile = wal.loadFromFile();
            for (WALEntry walEntry : logEntriesFromFile) {
                if (walEntry.isGreaterThan(offsetState.getCommittedOffset()))
                    break;

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

    private AutoCloseableLock readLeaderLock() {
        return AutoCloseableLock.acquire(leaderLock.readLock());
    }

    private AutoCloseableLock writeLeaderLock() {
        return AutoCloseableLock.acquire(leaderLock.writeLock());
    }

    private AutoCloseableLock writeReplicaLock() {
        return AutoCloseableLock.acquire(replicaLock.writeLock());
    }
}
