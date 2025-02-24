package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.client.adminrequests.ItemVersionsGetRequest;
import com.bteshome.keyvaluestore.client.adminresponses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.adminresponses.ItemVersionsGetResponse;
import com.bteshome.keyvaluestore.client.requests.*;
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
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class PartitionState implements AutoCloseable {
    private final String table;
    private final int partition;
    private final Duration timeToLive;
    private final String nodeId;
    private final Map<String, ItemEntry> data;
    private final Set<String> indexNames;
    private final Map<String, Map<String, Set<String>>> indexes;
    @Getter
    private final PriorityQueue<ItemExpiryKey> dataExpiryTimes;
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
        this.timeToLive = MetadataCache.getInstance().getTableTimeToLive(table);
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
        this.indexNames = MetadataCache.getInstance().getTableIndexNames(table);
        if (this.indexNames == null) {
            this.indexes = null;
        } else {
            this.indexes = new ConcurrentHashMap<>();
            for (String indexName : this.indexNames)
                this.indexes.put(indexName, new ConcurrentHashMap<>());
        }
        this.dataExpiryTimes = new PriorityQueue<>(Comparator.comparing(ItemExpiryKey::expiryTime));
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
            if (offsetState.getEndOffset().equals(LogPosition.ZERO))
                return;

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
                log.debug("Replication monitor for table '{}' partition '{}': current committed offset is '{}', new committed offset is '{}'.",
                        table,
                        partition,
                        committedOffset,
                        newCommittedOffset);
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
                if (request.isWithVersionCheck()) {
                    for (Item item : request.getItems()) {
                        String keyString = item.getKey();
                        if (!data.containsKey(keyString)) {
                            return ResponseEntity.ok(ItemPutResponse.builder()
                                    .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                                    .errorMessage("Unable to perform version check. Item with key %s does not exist.".formatted(keyString))
                                    .build());
                        }

                        LogPosition previousVersion = item.getPreviousVersion();
                        LogPosition version = data.get(keyString).valueVersions().getLast().offset();

                        if (previousVersion == null || previousVersion.equals(LogPosition.ZERO)){
                            return ResponseEntity.ok(ItemPutResponse.builder()
                                    .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                                    .errorMessage("Version has not been specified for the item with key %s.".formatted(keyString))
                                    .build());
                        }

                        if (!version.equals(previousVersion)){
                            return ResponseEntity.ok(ItemPutResponse.builder()
                                    .httpStatusCode(HttpStatus.CONFLICT.value())
                                    .errorMessage("The item with key %s has been modified.".formatted(keyString))
                                    .build());
                        }
                    }
                }

                long expiryTime = (timeToLive == null || timeToLive.equals(Duration.ZERO)) ? 0 : Instant.now().plus(timeToLive).toEpochMilli();
                long now = System.currentTimeMillis();
                List<LogPosition> itemOffsets = wal.appendPutOperation(leaderTerm, now, request.getItems(), expiryTime);
                LogPosition endOffset = itemOffsets.getLast();
                offsetState.setEndOffset(endOffset);

                for (int i = 0; i < request.getItems().size(); i++) {
                    Item item = request.getItems().get(i);
                    String keyString = item.getKey();
                    byte[] valueBytes = item.getValue();
                    LogPosition offset = itemOffsets.get(i);
                    ItemValueVersion itemValueVersion = ItemValueVersion.of(offset, valueBytes, expiryTime);

                    removeItemFromIndex(keyString);

                    data.compute(keyString, (key, value) -> {
                        if (value == null)
                            value = new ItemEntry(new CopyOnWriteArrayList<>(), this.indexes == null ? null : new ConcurrentHashMap<>());
                        value.valueVersions().add(itemValueVersion);
                        return value;
                    });

                    addItemToIndex(keyString, item.getIndexKeys());

                    if (expiryTime != 0L) {
                        ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, offset, expiryTime);
                        dataExpiryTimes.offer(itemExpiryKey);
                    }
                }

                return ResponseEntity.ok(ItemPutResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .endOffset(endOffset)
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

        return deleteItems(request.getKeys(), request.getAck());
    }

    public void deleteExpiredItems() {
        if (!isLeader)
            return;

        log.debug("Item expiration monitor about to check if any items have expired.");

        List<String> itemsToRemove = new ArrayList<>();
        long now = Instant.now().toEpochMilli();

        try (AutoCloseableLock l = writeLeaderLock()) {
            while (!dataExpiryTimes.isEmpty() && dataExpiryTimes.peek().expiryTime() <= now) {
                ItemExpiryKey itemExpiryKey = dataExpiryTimes.poll();
                itemsToRemove.add(itemExpiryKey.keyString());
            }
        }

        if (!itemsToRemove.isEmpty()) {
            log.debug("Expiring '{}' items.", itemsToRemove.size());
            deleteItems(itemsToRemove, null);
        }
    }

    private Mono<ResponseEntity<ItemDeleteResponse>> deleteItems(final List<String> itemKeys, AckType ack) {
        Mono<ResponseEntity<ItemDeleteResponse>> mono = Mono.fromCallable(() -> {
            try (AutoCloseableLock l = writeLeaderLock()) {
                long now = System.currentTimeMillis();
                List<LogPosition> itemOffsets = wal.appendDeleteOperation(leaderTerm, now, itemKeys);
                LogPosition endOffset = itemOffsets.getLast();
                offsetState.setEndOffset(endOffset);

                for (int i = 0; i < itemKeys.size(); i++) {
                    String keyString = itemKeys.get(i);
                    LogPosition offset = itemOffsets.get(i);
                    ItemValueVersion itemValueVersion = ItemValueVersion.of(offset, null, 0L);

                    data.compute(keyString, (key, value) -> {
                        if (value == null)
                            return null;
                        value.valueVersions().add(itemValueVersion);
                        return value;
                    });
                }

                return ResponseEntity.ok(ItemDeleteResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .endOffset(endOffset)
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
                            log.debug("Successfully committed DELETE operation for '{}' itemKeys to table '{}' partition '{}'.", itemKeys.size(), table, partition);
                            response.setHttpStatusCode(HttpStatus.OK.value());
                        }
                        else {
                            String errorMessage = "DELETE operation for '%s' itemKeys to table '%s' partition '%s' timed out.".formatted(itemKeys.size(), table, partition);
                            log.debug(errorMessage);
                            response.setHttpStatusCode(HttpStatus.REQUEST_TIMEOUT.value());
                        }
                    })
                    .map(c -> ResponseEntity.ok(response)));
        });
    }

    public Mono<ResponseEntity<ItemGetResponse>> getItem(ItemGetRequest request) {
        try {
            if (!isLeader) {
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
                return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                        .leaderEndpoint(leaderEndpoint)
                        .build()));
            }

            String key = request.getKey();

            if (!data.containsKey(key)) {
                return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build()));
            }

            List<ItemValueVersion> versions = data.get(key).valueVersions();
            ItemValueVersion version = null;

            if (request.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED)) {
                List<ItemValueVersion> committedVersions = versions
                        .stream()
                        .filter(v -> v.offset().isLessThanOrEquals(offsetState.getCommittedOffset()))
                        .toList();
                if (!committedVersions.isEmpty())
                        version = committedVersions.getLast();
            } else {
                version = versions.getLast();
            }

            if (version == null || version.bytes() == null) {
                return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build()));
            } else {
                return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .value(version.bytes())
                        .version(version.offset())
                        .build()));
            }
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
    }

    public Mono<ResponseEntity<ItemVersionsGetResponse>> getItemVersions(ItemVersionsGetRequest request) {
        try {
            if (!isLeader) {
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
                return Mono.just(ResponseEntity.ok(ItemVersionsGetResponse.builder()
                        .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                        .leaderEndpoint(leaderEndpoint)
                        .build()));
            }

            String key = request.getKey();

            if (!data.containsKey(key)) {
                return Mono.just(ResponseEntity.ok(ItemVersionsGetResponse.builder()
                        .httpStatusCode(HttpStatus.NOT_FOUND.value())
                        .build()));
            }

            List<byte[]> valueVersions = new ArrayList<>();
            data.get(key).valueVersions().forEach(version -> valueVersions.add(version.bytes()));

            return Mono.just(ResponseEntity.ok(ItemVersionsGetResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .valueVersions(valueVersions)
                    .build()));
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(ItemVersionsGetResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
    }

    public Mono<ResponseEntity<ItemListResponse>> listItems(ItemListRequest request) {
        try {
            if (!isLeader) {
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                        .leaderEndpoint(leaderEndpoint)
                        .build()));
            }

            List<Map.Entry<String, byte[]>> items = null;

            if (request.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED)) {
                LogPosition committedOffset = offsetState.getCommittedOffset();
                items = data
                        .entrySet()
                        .stream()
                        .map(e -> {
                            List<ItemValueVersion> committedVersions = e.getValue()
                                    .valueVersions()
                                    .stream()
                                    .filter(v -> v.offset().isLessThanOrEquals(committedOffset))
                                    .toList();
                            byte[] latestCommittedVersion = null;
                            if (!committedVersions.isEmpty())
                                latestCommittedVersion = committedVersions.getLast().bytes();
                            return latestCommittedVersion == null ? null : Map.entry(e.getKey(), latestCommittedVersion);
                        })
                        .filter(Objects::nonNull)
                        .limit(request.getLimit())
                        .toList();
            } else {
                items = data
                        .entrySet()
                        .stream()
                        .map(e -> {
                            byte[] latestVersion = e.getValue().valueVersions().getLast().bytes();
                            return latestVersion == null ? null : Map.entry(e.getKey(), latestVersion);
                        })
                        .filter(Objects::nonNull)
                        .limit(request.getLimit())
                        .toList();
            }

            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(items)
                    .build()));
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
    }

    public Mono<ResponseEntity<ItemListResponse>> queryForItems(ItemQueryRequest request) {
        try {
            if (!isLeader) {
                String leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                        .leaderEndpoint(leaderEndpoint)
                        .build()));
            }

            if (this.indexes == null || !this.indexes.containsKey(request.getIndexName())) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                        .errorMessage("Index '%s' does not exist.".formatted(request.getIndexName()))
                        .build()));
            }

            Map<String, Set<String>> index = this.indexes.get(request.getIndexName());

            if (!index.containsKey(request.getIndexKey())) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .items(new ArrayList<>())
                        .build()));
            }

            Set<String> itemKeys = index.get(request.getIndexKey());
            List<Map.Entry<String, byte[]>> items = new ArrayList<>();
            LogPosition committedOffset = offsetState.getCommittedOffset();

            for (String key : itemKeys) {
                List<ItemValueVersion> versions = data.get(key).valueVersions();
                ItemValueVersion version = null;

                if (request.getIsolationLevel().equals(IsolationLevel.READ_COMMITTED)) {
                    List<ItemValueVersion> committedVersions = versions
                            .stream()
                            .filter(v -> v.offset().isLessThanOrEquals(committedOffset))
                            .toList();
                    if (!committedVersions.isEmpty())
                        version = committedVersions.getLast();
                } else {
                    version = versions.getLast();
                }

                if (!(version == null || version.bytes() == null)) {
                    items.add(Map.entry(key, version.bytes()));
                }
            }

            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(items)
                    .build()));
        } catch (Exception e) {
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .errorMessage(e.getMessage())
                    .build()));
        }
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
        try (AutoCloseableLock l = writeReplicaLock()) {
            LogPosition currentCommitedOffset = offsetState.getCommittedOffset();

            if (!logEntries.isEmpty()) {
                wal.appendLogs(logEntries);
                offsetState.setEndOffset(logEntries.getLast().getOffset());
            }

            if (commitedOffset.equals(LogPosition.ZERO) || commitedOffset.equals(currentCommitedOffset))
                return;

            List<WALEntry> logEntriesNotAppliedYet = wal.readLogs(currentCommitedOffset, commitedOffset);
            for (WALEntry walEntry : logEntriesNotAppliedYet) {
                String keyString = new String(walEntry.key());
                Map<String, String> entryIndexKeys = walEntry.indexes() == null ?
                        null :
                        JsonSerDe.deserialize(walEntry.indexes(), new TypeReference<>(){});
                LogPosition offset = walEntry.getOffset();
                ItemValueVersion itemValueVersion = ItemValueVersion.of(offset, walEntry.value(), walEntry.expiryTime());

                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        removeItemFromIndex(keyString);
                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                value = new ItemEntry(new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().add(itemValueVersion);
                            if (entryIndexKeys != null)
                                value.indexKeys().putAll(entryIndexKeys);
                            return value;
                        });
                        if (walEntry.expiryTime() != 0L) {
                            ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, offset, walEntry.expiryTime());
                            dataExpiryTimes.offer(itemExpiryKey);
                        }
                        addItemToIndex(keyString, entryIndexKeys);
                    }
                    case OperationType.DELETE -> {
                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                return null;
                            value.valueVersions().add(itemValueVersion);
                            return value;
                        });
                    }
                }
            }

            offsetState.setCommittedOffset(commitedOffset);
        }
    }

    public void applyDataSnapshot(DataSnapshot dataSnapshot) {
        try (AutoCloseableLock l = writeReplicaLock()) {
            offsetState.setEndOffset(dataSnapshot.getLastCommittedOffset());
            offsetState.setCommittedOffset(dataSnapshot.getLastCommittedOffset());

            for (Map.Entry<String, ItemEntry> entry : dataSnapshot.getData().entrySet()) {
                String keyString = entry.getKey();
                Map<String, String> entryIndexKeys = entry.getValue().indexKeys();
                List<ItemValueVersion> valueVersions = entry.getValue().valueVersions();

                removeItemFromIndex(keyString);

                data.compute(keyString, (key, value) -> {
                    if (value == null)
                        value = new ItemEntry(new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                    value.valueVersions().addAll(valueVersions);
                    if (entryIndexKeys != null)
                        value.indexKeys().putAll(entryIndexKeys);
                    return value;
                });

                for (ItemValueVersion itemValueVersion : valueVersions) {
                    if (itemValueVersion.expiryTime() != 0L) {
                        ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(entry.getKey(), itemValueVersion.offset(), itemValueVersion.expiryTime());
                        dataExpiryTimes.offer(itemExpiryKey);
                    }
                }

                addItemToIndex(keyString, entryIndexKeys);
            }
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

                    for (Map.Entry<String, ItemEntry> entry : data.entrySet()) {
                        while (!entry.getValue().valueVersions().isEmpty()) {
                            ItemValueVersion latestVersion = entry.getValue().valueVersions().getLast();
                            if (latestVersion.offset().isGreaterThan(earliestISREndOffset)) {
                                entry.getValue().valueVersions().removeLast();
                                ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(entry.getKey(), latestVersion.offset(), latestVersion.expiryTime());
                                dataExpiryTimes.remove(itemExpiryKey);
                            } else {
                                break;
                            }
                        }
                        if (entry.getValue().valueVersions().isEmpty()) {
                            String keyString = entry.getKey();
                            if (entry.getValue().indexKeys() != null) {
                                for (Map.Entry<String, String> indexKey : entry.getValue().indexKeys().entrySet()) {
                                    Map<String, Set<String>> index = this.indexes.get(indexKey.getKey());
                                    index.get(indexKey.getValue()).remove(keyString);
                                }
                            }
                            data.remove(keyString);
                        }
                    }
                }

                if (earliestISREndOffset.isGreaterThan(thisReplicaCommittedOffset))
                    offsetState.setCommittedOffset(earliestISREndOffset);

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

            HashMap<String, ItemEntry> dataCopy = new HashMap<>(data);
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
                    for (Map.Entry<String, ItemEntry> entry : dataSnapshot.getData().entrySet()) {
                        removeItemFromIndex(entry.getKey());

                        data.compute(entry.getKey(), (key, value) -> {
                            value = new ItemEntry(new CopyOnWriteArrayList<>(), this.indexes == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().addAll(entry.getValue().valueVersions());
                            if (this.indexes != null && entry.getValue().indexKeys() != null)
                                value.indexKeys().putAll(entry.getValue().indexKeys());
                            return value;
                        });

                        for (ItemValueVersion itemValueVersion : entry.getValue().valueVersions()) {
                            if (itemValueVersion.expiryTime() != 0L) {
                                ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(entry.getKey(), itemValueVersion.offset(), itemValueVersion.expiryTime());
                                dataExpiryTimes.offer(itemExpiryKey);
                            }
                        }

                        addItemToIndex(entry.getKey(), entry.getValue().indexKeys());
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

                String keyString = new String(walEntry.key());
                Map<String, String> entryIndexKeys = walEntry.indexes() == null ?
                        null :
                        JsonSerDe.deserialize(walEntry.indexes(), new TypeReference<>(){});
                ItemValueVersion itemValueVersion = ItemValueVersion.of(walEntry.getOffset(), walEntry.value(), walEntry.expiryTime());

                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        removeItemFromIndex(keyString);
                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                value = new ItemEntry(new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().add(itemValueVersion);
                            if (entryIndexKeys != null)
                                value.indexKeys().putAll(entryIndexKeys);
                            return value;
                        });
                        if (walEntry.expiryTime() != 0L) {
                            ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, itemValueVersion.offset(), itemValueVersion.expiryTime());
                            dataExpiryTimes.offer(itemExpiryKey);
                        }
                        addItemToIndex(keyString, entryIndexKeys);
                    }
                    case OperationType.DELETE -> {
                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                return null;
                            value.valueVersions().add(itemValueVersion);
                            return value;
                        });
                    }
                }
            }

            log.debug("Loaded '{}' log entries from WAL file for table '{}' partition '{}'.",
                      logEntriesFromFile.size(),
                      table,
                      partition);
        }
    }

    private void removeItemFromIndex(String keyString) {
        if (data.containsKey(keyString)) {
            Map<String, String> existingIndexKeys = data.get(keyString).indexKeys();
            if (existingIndexKeys != null) {
                for (Map.Entry<String, String> existingIndexKey : existingIndexKeys.entrySet()) {
                    Map<String, Set<String>> index = this.indexes.get(existingIndexKey.getKey());
                    index.get(existingIndexKey.getValue()).remove(keyString);
                }
                existingIndexKeys.clear();
            }
        }
    }

    private void addItemToIndex(String keyString, Map<String, String> newIndexKeys) {
        if (this.indexes != null && newIndexKeys != null) {
            for (String indexName : this.indexNames) {
                if (!newIndexKeys.containsKey(indexName))
                    continue;

                this.indexes.get(indexName).compute(newIndexKeys.get(indexName), (key, value) -> {
                    if (value == null)
                        value = ConcurrentHashMap.newKeySet();
                    value.add(keyString);
                    return value;
                });

                this.data.get(keyString).indexKeys().put(indexName, newIndexKeys.get(indexName));
            }
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
