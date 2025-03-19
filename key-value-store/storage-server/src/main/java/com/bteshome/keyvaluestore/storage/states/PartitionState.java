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
import com.bteshome.keyvaluestore.storage.api.grpc.client.WalFetcherGrpc;
import com.bteshome.keyvaluestore.storage.common.ChecksumUtil;
import com.bteshome.keyvaluestore.storage.common.CompressionUtil;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import com.bteshome.keyvaluestore.storage.core.ISRSynchronizer;
import com.bteshome.keyvaluestore.storage.core.ReplicationMonitor;
import com.bteshome.keyvaluestore.storage.core.WALFetcher;
import com.bteshome.keyvaluestore.storage.entities.*;
import com.bteshome.keyvaluestore.storage.proto.LogPositionProto;
import com.bteshome.keyvaluestore.storage.proto.WalEntryProto;
import com.bteshome.keyvaluestore.storage.proto.WalFetchPayloadTypeProto;
import com.bteshome.keyvaluestore.storage.proto.WalFetchResponseProto;
import com.bteshome.keyvaluestore.storage.requests.WALGetReplicaEndOffsetRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchPayloadType;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.responses.WALGetReplicaEndOffsetResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.ByteString;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class PartitionState implements AutoCloseable {
    private final String table;
    private final int partition;
    private final Duration timeToLive;
    private final String nodeId;
    private final Map<String, ItemEntry> data;
    private final Set<String> secondaryIndexNames;
    private final NavigableSet<String> primaryIndex;
    private final Map<String, Map<String, NavigableSet<String>>> secondaryIndexes;
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
    private Tuple<String, Integer> leaderGrpcEndpoint;
    private boolean isLeader;
    private int leaderTerm;
    private Set<String> inSyncReplicas;
    private WalFetcherGrpc walFetcherGrpc;

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
        this.storageSettings = storageSettings;
        this.isrSynchronizer = isrSynchronizer;
        this.webClient = webClient;
        this.leaderLock = new ReentrantReadWriteLock(true);
        this.replicaLock = new ReentrantReadWriteLock(true);
        this.data = new ConcurrentHashMap<>();
        this.primaryIndex = new ConcurrentSkipListSet<>();
        this.secondaryIndexNames = MetadataCache.getInstance().getTableIndexNames(table);
        if (this.secondaryIndexNames == null) {
            this.secondaryIndexes = null;
        } else {
            this.secondaryIndexes = new ConcurrentHashMap<>();
            for (String indexName : this.secondaryIndexNames)
                this.secondaryIndexes.put(indexName, new ConcurrentHashMap<>());
        }
        this.dataExpiryTimes = new PriorityQueue<>(Comparator.comparing(ItemExpiryKey::expiryTime));
        this.replicationMonitorSink = Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);
        createPartitionDirectoryIfNotExists();
        wal = new WAL(storageSettings.getNode().getStorageDir(), table, partition);
        dataSnapshotFile = "%s/%s-%s/data.ser.snappy".formatted(storageSettings.getNode().getStorageDir(), table, partition);
        offsetState = new OffsetState(table, partition, storageSettings);
        loadFromDataSnapshotAndWALFile();
        if (isLeader) {
            this.inSyncReplicas = MetadataCache.getInstance().getInSyncReplicas(table, partition);
        } else {
            this.leaderEndpoint = MetadataCache.getInstance().getLeaderEndpoint(table, partition);
            this.leaderGrpcEndpoint = MetadataCache.getInstance().getLeaderGrpcEndpoint(table, partition);
            startWalFetcherGrpc();
        }
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

                    data.compute(keyString, (key, value) -> {
                        if (value == null)
                            value = new ItemEntry(item.getPartitionKey(), new CopyOnWriteArrayList<>(), this.secondaryIndexes == null ? null : new ConcurrentHashMap<>());
                        value.valueVersions().add(itemValueVersion);
                        return value;
                    });

                    Mono.fromRunnable(() -> {
                            removeItemFromIndexes(keyString);
                            addItemToIndexes(keyString, item.getIndexKeys());

                            if (expiryTime != 0L) {
                                ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, offset, expiryTime);
                                dataExpiryTimes.offer(itemExpiryKey);
                            }
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe();
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

        List<String> itemsToRemove = new ArrayList<>();
        LogPosition committedOffset = offsetState.getCommittedOffset();

        for (String key : request.getKeys()) {
            if (!data.containsKey(key))
                continue;

            ItemValueVersion latestVersion = data.get(key).valueVersions().getLast();

            if (latestVersion.offset().isGreaterThan(committedOffset))
                continue;

            if (latestVersion.bytes() == null)
                continue;

            itemsToRemove.add(key);
        }

        if (itemsToRemove.isEmpty()) {
            return Mono.just(ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .build()));
        }

        return deleteItems(itemsToRemove, request.getAck());
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

        Mono.delay(Duration.ofMinutes(1))
                .then(Mono.fromRunnable(this::removeDeletedItems))
                .subscribe();
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

            if (primaryIndex.isEmpty()) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .items(new ArrayList<>())
                        .build()));
            }

            if (request.getLimit() == 0) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                        .errorMessage("Limit cannot be zero.")
                        .build()));
            }

            List<ItemResponse<byte[]>> items = new ArrayList<>();
            SortedSet<String> subset = primaryIndex;
            List<String> cursorKeys = new ArrayList<>(request.getLimit());
            LogPosition committedOffset = offsetState.getCommittedOffset();
            LogPosition endOffset = offsetState.getEndOffset();
            boolean hasMore = false;

            if (request.getLastReadItemKey() != null) {
                String lastReadItemKey = request.getLastReadItemKey().strip();
                if (primaryIndex.contains(lastReadItemKey))
                    subset = primaryIndex.tailSet(lastReadItemKey);
            }

            for (String key : subset) {
                if (cursorKeys.size() >= request.getLimit()) {
                    hasMore = primaryIndex.tailSet(cursorKeys.getLast()).size() > 1;
                    break;
                }
                if (key.equalsIgnoreCase(request.getLastReadItemKey()))
                    continue;
                cursorKeys.add(key);
            }

            for (String key : cursorKeys) {
                ItemEntry itemEntry = data.get(key);
                List<ItemValueVersion> versions = itemEntry.valueVersions();
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
                    items.add(new ItemResponse<>(key, itemEntry.partitionKey(), version.bytes()));
                }
            }

            CursorPosition cursorPosition = new CursorPosition();
            if (!items.isEmpty()) {
                cursorPosition.setLastReadItemKey(items.getLast().getItemKey());
                cursorPosition.setPartition(partition);
                cursorPosition.setHasMore(hasMore);
            }

            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(items)
                    .cursorPosition(cursorPosition)
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

            if (this.secondaryIndexes == null || !this.secondaryIndexes.containsKey(request.getIndexName())) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                        .errorMessage("Index '%s' does not exist.".formatted(request.getIndexName()))
                        .build()));
            }

            if (request.getLimit() == 0) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.BAD_REQUEST.value())
                        .errorMessage("Limit cannot be zero.")
                        .build()));
            }

            Map<String, NavigableSet<String>> index = this.secondaryIndexes.get(request.getIndexName());

            if (!index.containsKey(request.getIndexKey())) {
                return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                        .httpStatusCode(HttpStatus.OK.value())
                        .items(new ArrayList<>())
                        .build()));
            }

            SortedSet<String> itemKeys = index.get(request.getIndexKey());
            List<String> cursorKeys = new ArrayList<>(request.getLimit());
            List<ItemResponse<byte[]>> items = new ArrayList<>();
            LogPosition committedOffset = offsetState.getCommittedOffset();
            LogPosition endOffset = offsetState.getEndOffset();
            boolean hasMore = false;

            if (request.getLastReadItemKey() != null) {
                String lastReadItemKey = request.getLastReadItemKey().strip();
                if (itemKeys.contains(lastReadItemKey))
                    itemKeys = itemKeys.tailSet(lastReadItemKey);
            }

            for (String key : itemKeys) {
                if (cursorKeys.size() >= request.getLimit()) {
                    hasMore = primaryIndex.tailSet(cursorKeys.getLast()).size() > 1;
                    break;
                }
                if (key.equalsIgnoreCase(request.getLastReadItemKey()))
                    continue;
                cursorKeys.add(key);
            }

            for (String key : cursorKeys) {
                ItemEntry itemEntry = data.get(key);
                List<ItemValueVersion> versions = itemEntry.valueVersions();
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
                    items.add(new ItemResponse<>(key, itemEntry.partitionKey(), version.bytes()));
                }
            }

            CursorPosition cursorPosition = new CursorPosition();
            if (!items.isEmpty()) {
                cursorPosition.setLastReadItemKey(items.getLast().getItemKey());
                cursorPosition.setPartition(partition);
                cursorPosition.setHasMore(hasMore);
            }
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.OK.value())
                    .items(items)
                    .cursorPosition(cursorPosition)
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

    public WalFetchResponseProto getLogEntriesProto(LogPosition lastFetchOffset,
                                                    LogPosition replicaCommittedOffset,
                                                    int maxNumRecords,
                                                    String replicaId) {
        if (!isLeader) {
            return WalFetchResponseProto.newBuilder()
                    .setHttpStatusCode(HttpStatus.MOVED_PERMANENTLY.value())
                    .build();
        }

        offsetState.setReplicaEndOffset(replicaId, lastFetchOffset);

        try (AutoCloseableLock l = readLeaderLock()) {
            LogPosition walStartOffset = wal.getStartOffset();
            LogPosition endOffset = offsetState.getEndOffset();
            LogPosition commitedOffset = offsetState.getCommittedOffset();
            LogPosition previousLeaderEndOffset = offsetState.getPreviousLeaderEndOffset();
            maxNumRecords = Math.min(maxNumRecords, this.fetchMaxNumRecords);

            LogPositionProto commitedOffsetProto = LogPositionProto.newBuilder()
                    .setTerm(commitedOffset.leaderTerm())
                    .setIndex(commitedOffset.index())
                    .build();

            LogPositionProto previousLeaderEndOffsetProto = LogPositionProto.newBuilder()
                    .setTerm(previousLeaderEndOffset.leaderTerm())
                    .setIndex(previousLeaderEndOffset.index())
                    .build();

            log.trace("Received WAL fetch request from replica '{}' for table '{}' partition '{}'. Last fetch offset = {}, " +
                            "leader end offset = {}, replica committed offset = {}, leader committed offset = {}.",
                    replicaId,
                    table,
                    partition,
                    lastFetchOffset,
                    endOffset,
                    replicaCommittedOffset,
                    commitedOffset);

            if (endOffset.equals(LogPosition.ZERO))
                return WalFetchResponseProto.getDefaultInstance();

            if (lastFetchOffset.equals(endOffset)) {
                if (replicaCommittedOffset.equals(commitedOffset))
                    return WalFetchResponseProto.getDefaultInstance();
                return WalFetchResponseProto.newBuilder()
                        .setHttpStatusCode(HttpStatus.OK.value())
                        .setCommitedOffset(commitedOffsetProto)
                        .build();
            }

            if (lastFetchOffset.equals(LogPosition.ZERO)) {
                if (walStartOffset.equals(LogPosition.ZERO))
                    return getSnapshotProto(lastFetchOffset, walStartOffset, endOffset);
                return getLogsProto(lastFetchOffset, maxNumRecords, commitedOffsetProto);
            } else if (lastFetchOffset.leaderTerm() == leaderTerm) {
                if (walStartOffset.equals(LogPosition.ZERO) || lastFetchOffset.index() < walStartOffset.index() - 1)
                    return getSnapshotProto(lastFetchOffset, walStartOffset, endOffset);
                return getLogsProto(lastFetchOffset, maxNumRecords, commitedOffsetProto);
            } else if (lastFetchOffset.leaderTerm() == leaderTerm - 1) {
                if (lastFetchOffset.isGreaterThan(previousLeaderEndOffset)) {
                    return WalFetchResponseProto.newBuilder()
                            .setHttpStatusCode(HttpStatus.CONFLICT.value())
                            .setTruncateToOffset(previousLeaderEndOffsetProto)
                            .build();
                }
                if (walStartOffset.equals(LogPosition.ZERO) || walStartOffset.leaderTerm() > lastFetchOffset.leaderTerm())
                    return getSnapshotProto(lastFetchOffset, walStartOffset, endOffset);
                return getLogsProto(lastFetchOffset, maxNumRecords, commitedOffsetProto);
            } else {
                log.warn("Replica '{}' requested log entries with a last fetch leader term {} but current leader term is {}.",
                        replicaId,
                        lastFetchOffset.leaderTerm(),
                        leaderTerm);
                if (walStartOffset.equals(LogPosition.ZERO) || walStartOffset.leaderTerm() > lastFetchOffset.leaderTerm())
                    return getSnapshotProto(lastFetchOffset, walStartOffset, endOffset);
                return getLogsProto(lastFetchOffset, maxNumRecords, commitedOffsetProto);
            }
        } catch (Exception e) {
            return WalFetchResponseProto.newBuilder()
                    .setHttpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .setErrorMessage(e.getMessage())
                    .build();
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

    private WalFetchResponseProto getLogsProto(LogPosition lastFetchOffset, int maxNumRecords, LogPositionProto commitedOffsetProto) {
        List<WalEntryProto> entries = wal.readLogsProto(lastFetchOffset, maxNumRecords);

        return WalFetchResponseProto.newBuilder()
                .setHttpStatusCode(HttpStatus.OK.value())
                .addAllEntries(entries)
                .setCommitedOffset(commitedOffsetProto)
                .setPayloadType(WalFetchPayloadTypeProto.LOG)
                .build();
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

    private WalFetchResponseProto getSnapshotProto(LogPosition lastFetchOffset, LogPosition walStartOffset, LogPosition endOffset) {
        byte[] dataSnapshotBytes = readDataSnapshotBytes();

        if (dataSnapshotBytes == null) {
            String errorMessage = "No WAL or data snapshot found after the requested offset %s. Current wal start offset is %s: and leader end offset is: %s".formatted(
                    lastFetchOffset,
                    walStartOffset,
                    endOffset);
            return WalFetchResponseProto.newBuilder()
                    .setHttpStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                    .setErrorMessage(errorMessage)
                    .build();
        }

        return WalFetchResponseProto.newBuilder()
                .setHttpStatusCode(HttpStatus.OK.value())
                .setPayloadType(WalFetchPayloadTypeProto.SNAPSHOT)
                .setDataSnapshotBytes(ByteString.copyFrom(dataSnapshotBytes))
                .build();
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
            if (!logEntries.isEmpty()) {
                wal.appendLogs(logEntries);
                offsetState.setEndOffset(logEntries.getLast().getOffset());
            }

            for (WALEntry walEntry : logEntries) {
                String keyString = new String(walEntry.key());
                String partitionKeyString = walEntry.partitionKey() == null ?
                        null :
                        new String(walEntry.partitionKey());
                Map<String, String> entryIndexKeys = walEntry.indexes() == null ?
                        null :
                        JsonSerDe.deserialize(walEntry.indexes(), new TypeReference<>(){});
                LogPosition offset = walEntry.getOffset();
                ItemValueVersion itemValueVersion = ItemValueVersion.of(offset, walEntry.value(), walEntry.expiryTime());

                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        removeItemFromIndexes(keyString);

                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                value = new ItemEntry(partitionKeyString, new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().add(itemValueVersion);
                            return value;
                        });

                        addItemToIndexes(keyString, entryIndexKeys);

                        if (walEntry.expiryTime() != 0L) {
                            ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, offset, walEntry.expiryTime());
                            dataExpiryTimes.offer(itemExpiryKey);
                        }
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

            LogPosition currentCommitedOffset = offsetState.getCommittedOffset();

            if (commitedOffset.equals(LogPosition.ZERO) || commitedOffset.equals(currentCommitedOffset))
                return;

            offsetState.setCommittedOffset(commitedOffset);
        }
    }

    public void appendLogEntriesProto(List<WalEntryProto> logEntries, LogPosition commitedOffset) {
        try (AutoCloseableLock l = writeReplicaLock()) {
            if (!logEntries.isEmpty()) {
                wal.appendLogsProto(logEntries);
                LogPosition endOffset = LogPosition.of(logEntries.getLast().getTerm(), logEntries.getLast().getIndex());
                offsetState.setEndOffset(endOffset);
            }

            for (WalEntryProto walEntry : logEntries) {
                String keyString = new String(walEntry.getKey().toByteArray());
                String partitionKeyString = walEntry.getPartitionKey().isEmpty() ?
                        null :
                        new String(walEntry.getPartitionKey().toByteArray());
                Map<String, String> entryIndexKeys = walEntry.getIndexes().isEmpty() ?
                        null :
                        JsonSerDe.deserialize(walEntry.getIndexes().toByteArray(), new TypeReference<>(){});
                LogPosition offset = LogPosition.of(walEntry.getTerm(), walEntry.getIndex());
                byte[] valueBytes = walEntry.getValue().isEmpty() ?
                        null :
                        walEntry.getValue().toByteArray();
                ItemValueVersion itemValueVersion = ItemValueVersion.of(offset, valueBytes, walEntry.getExpiryTime());

                switch ((byte)walEntry.getOperationType()) {
                    case OperationType.PUT -> {
                        removeItemFromIndexes(keyString);

                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                value = new ItemEntry(partitionKeyString, new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().add(itemValueVersion);
                            return value;
                        });

                        addItemToIndexes(keyString, entryIndexKeys);

                        if (walEntry.getExpiryTime() != 0L) {
                            ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, offset, walEntry.getExpiryTime());
                            dataExpiryTimes.offer(itemExpiryKey);
                        }
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

            LogPosition currentCommitedOffset = offsetState.getCommittedOffset();

            if (commitedOffset.equals(LogPosition.ZERO) || commitedOffset.equals(currentCommitedOffset))
                return;

            offsetState.setCommittedOffset(commitedOffset);
        }
    }

    public void applyDataSnapshot(DataSnapshot dataSnapshot) {
        try (AutoCloseableLock l = writeReplicaLock()) {
            offsetState.setEndOffset(dataSnapshot.getLastCommittedOffset());
            offsetState.setCommittedOffset(dataSnapshot.getLastCommittedOffset());

            for (Map.Entry<String, ItemEntry> entry : dataSnapshot.getData().entrySet()) {
                String keyString = entry.getKey();
                String partitionKeyString = entry.getValue().partitionKey();
                Map<String, String> entryIndexKeys = entry.getValue().indexKeys();
                List<ItemValueVersion> valueVersions = entry.getValue().valueVersions();

                removeItemFromIndexes(keyString);

                data.compute(keyString, (key, value) -> {
                    if (value == null)
                        value = new ItemEntry(partitionKeyString, new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                    value.valueVersions().addAll(valueVersions);
                    return value;
                });

                addItemToIndexes(keyString, entryIndexKeys);

                for (ItemValueVersion itemValueVersion : valueVersions) {
                    if (itemValueVersion.expiryTime() != 0L) {
                        ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(entry.getKey(), itemValueVersion.offset(), itemValueVersion.expiryTime());
                        dataExpiryTimes.offer(itemExpiryKey);
                    }
                }
            }
        }
    }

    private void startWalFetcherGrpc() {
        if (storageSettings.isGrpcEnabled()) {
            walFetcherGrpc = new WalFetcherGrpc(
                    this,
                    nodeId,
                    table,
                    partition,
                    leaderGrpcEndpoint,
                    fetchMaxNumRecords);
            walFetcherGrpc.start();
        }
    }

    private void stopWalFetcherGrpc() {
        if (walFetcherGrpc != null) {
            walFetcherGrpc.stop();
            walFetcherGrpc = null;
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
            this.stopWalFetcherGrpc();

            if (nodeId.equals(request.getNewLeaderId())) {
                this.isLeader = true;
                this.leaderTerm = request.getNewLeaderTerm();
                this.leaderEndpoint = null;
                this.leaderGrpcEndpoint = null;
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
                                    Map<String, NavigableSet<String>> index = this.secondaryIndexes.get(indexKey.getKey());
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
                this.leaderGrpcEndpoint = MetadataCache.getInstance().getGrpcEndpoint(request.getNewLeaderId());
                this.inSyncReplicas = null;
                this.startWalFetcherGrpc();
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

    private void removeDeletedItems() {
        try (AutoCloseableLock l = writeLeaderLock();
             AutoCloseableLock l2 = writeReplicaLock();) {
            long count = 0;
            LogPosition committedOffset = offsetState.getCommittedOffset();

            for (Map.Entry<String, ItemEntry> item : data.entrySet()) {
                String keyString = item.getKey();
                ItemValueVersion latestVersion = item.getValue().valueVersions().getLast();
                if (latestVersion.offset().isLessThanOrEquals(committedOffset)) {
                    if (latestVersion.bytes() == null) {
                        count++;
                        removeItemFromIndexes(keyString);
                        for (ItemValueVersion version : item.getValue().valueVersions()) {
                            if (version.expiryTime() != 0) {
                                ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, version.offset(), version.expiryTime());
                                dataExpiryTimes.remove(itemExpiryKey);
                            }
                        }
                        data.remove(keyString);
                    }
                }
            }

            if (count > 0)
                log.info("Removed {} deleted items for table '{}' partition '{}'.", count, table, partition);
        } catch (Exception e) {
            String errorMessage = "Error removing deleted items for table '%s' partition '%s'.".formatted(table, partition);
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
                        removeItemFromIndexes(entry.getKey());

                        data.compute(entry.getKey(), (key, value) -> {
                            value = new ItemEntry(entry.getValue().partitionKey(), new CopyOnWriteArrayList<>(), this.secondaryIndexes == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().addAll(entry.getValue().valueVersions());
                            return value;
                        });

                        addItemToIndexes(entry.getKey(), entry.getValue().indexKeys());

                        for (ItemValueVersion itemValueVersion : entry.getValue().valueVersions()) {
                            if (itemValueVersion.expiryTime() != 0L) {
                                ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(entry.getKey(), itemValueVersion.offset(), itemValueVersion.expiryTime());
                                dataExpiryTimes.offer(itemExpiryKey);
                            }
                        }
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
                String partitionKeyString = walEntry.partitionKey() == null ?
                        null :
                        new String(walEntry.partitionKey());
                Map<String, String> entryIndexKeys = walEntry.indexes() == null ?
                        null :
                        JsonSerDe.deserialize(walEntry.indexes(), new TypeReference<>(){});
                ItemValueVersion itemValueVersion = ItemValueVersion.of(walEntry.getOffset(), walEntry.value(), walEntry.expiryTime());

                switch (walEntry.operation()) {
                    case OperationType.PUT -> {
                        removeItemFromIndexes(keyString);
                        data.compute(keyString, (key, value) -> {
                            if (value == null)
                                value = new ItemEntry(partitionKeyString, new CopyOnWriteArrayList<>(), entryIndexKeys == null ? null : new ConcurrentHashMap<>());
                            value.valueVersions().add(itemValueVersion);
                            return value;
                        });
                        addItemToIndexes(keyString, entryIndexKeys);
                        if (walEntry.expiryTime() != 0L) {
                            ItemExpiryKey itemExpiryKey = ItemExpiryKey.of(keyString, itemValueVersion.offset(), itemValueVersion.expiryTime());
                            dataExpiryTimes.offer(itemExpiryKey);
                        }
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

    private void removeItemFromIndexes(String keyString) {
        if (data.containsKey(keyString)) {
            this.primaryIndex.remove(keyString);

            Map<String, String> existingSecondaryIndexKeys = data.get(keyString).indexKeys();
            if (existingSecondaryIndexKeys != null) {
                for (Map.Entry<String, String> existingIndexKey : existingSecondaryIndexKeys.entrySet()) {
                    Map<String, NavigableSet<String>> index = this.secondaryIndexes.get(existingIndexKey.getKey());
                    index.get(existingIndexKey.getValue()).remove(keyString);
                }
                existingSecondaryIndexKeys.clear();
            }
        }
    }

    private void addItemToIndexes(String keyString, Map<String, String> newSecondaryIndexKeys) {
        this.primaryIndex.add(keyString);

        if (this.secondaryIndexes != null && newSecondaryIndexKeys != null) {
            for (String indexName : this.secondaryIndexNames) {
                if (!newSecondaryIndexKeys.containsKey(indexName))
                    continue;

                this.secondaryIndexes.get(indexName).compute(newSecondaryIndexKeys.get(indexName), (key, value) -> {
                    if (value == null)
                        value = new ConcurrentSkipListSet<>();
                    value.add(keyString);
                    return value;
                });

                this.data.get(keyString).indexKeys().put(indexName, newSecondaryIndexKeys.get(indexName));
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
