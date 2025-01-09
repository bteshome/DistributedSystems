package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.StorageNodeState;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.*;
import com.bteshome.keyvaluestore.common.responses.*;
import com.bteshome.keyvaluestore.common.requests.RequestType;
import com.bteshome.keyvaluestore.common.entities.EntityType;
import com.bteshome.keyvaluestore.common.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.*;
import org.springframework.http.HttpStatus;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MetadataStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<EntityType, Map<String, Object>> state;
    private final MetadataSettings metadataSettings;
    private ScheduledExecutorService heartBeatExecutor = null;
    private static final String CURRENT = "current";

    public MetadataStateMachine(MetadataSettings metadataSettings) {
        this.metadataSettings = metadataSettings;
        this.state = new ConcurrentHashMap<>();
        state.put(EntityType.TABLE, new HashMap<>());
        state.put(EntityType.STORAGE_NODE, new HashMap<>());
        state.put(EntityType.CONFIGURATION, new HashMap<>());
        state.put(EntityType.VERSION, new HashMap<>());
        state.get(EntityType.VERSION).put(CURRENT, 0L);
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    private void incrementVersion() {
        state.get(EntityType.VERSION).put(CURRENT, Long.parseLong(state.get(EntityType.VERSION).get(CURRENT).toString()) + 1L);
    }

    private void scheduleStorageNodeHeartbeatMonitor() {
        long monitorIntervalMs = (Long)state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_HEARTBEAT_MONITOR_INTERVAL_MS_KEY);
        long expectIntervalMs = (Long)state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_HEARTBEAT_EXPECT_INTERVAL_MS_KEY);

        try {
            heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();
            heartBeatExecutor.scheduleAtFixedRate(() -> {
                    try (AutoCloseableLock lock = writeLock()) {
                        boolean statusChanged = new StorageNodeHeartbeatMonitor().checkStatus(state.get(EntityType.STORAGE_NODE), expectIntervalMs);
                        if (statusChanged) {
                            // TODO
                            //  - add changes to the transaction log
                            //  - update the last applied term index
                        }
                    }
                },
                monitorIntervalMs,
                monitorIntervalMs,
                TimeUnit.MILLISECONDS);
            log.info("Scheduled storage node heartbeat monitor. The interval is {} ms.", monitorIntervalMs);
        } catch (Exception e) {
            log.error("Error scheduling storage node monitor: ", e);
        }
    }

    private void reset() {
        state.get(EntityType.TABLE).clear();
        state.get(EntityType.STORAGE_NODE).clear();
        setLastAppliedTermIndex(null);
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    @Override
    public void close() throws IOException {
        if (heartBeatExecutor != null) {
            heartBeatExecutor.close();
        }
        super.close();
    }

    @Override
    public LeaderEventApi leaderEvent() {
        if (state.get(EntityType.CONFIGURATION).isEmpty()) {
            try (AutoCloseableLock lock = writeLock()) {
                new ConfigurationLoader().load(state, metadataSettings);
            }
        }
        scheduleStorageNodeHeartbeatMonitor();
        return super.leaderEvent();
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        this.storage.init(storage);
        loadSnapshot(this.storage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException {
        close();
        loadSnapshot(storage.loadLatestSnapshot());
    }

    public long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            log.warn("The snapshot info is null.");
            return RaftLog.INVALID_LOG_INDEX;
        }

        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            log.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
            return RaftLog.INVALID_LOG_INDEX;
        }

        final MD5Hash md5 = snapshot.getFile().getFileDigest();
        if (md5 != null) {
            MD5FileUtil.verifySavedMD5(snapshotFile, md5);
        }

        final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
        try (AutoCloseableLock lock = writeLock();
             ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
                     FileUtils.newInputStream(snapshotFile)))) {
            reset();
            setLastAppliedTermIndex(last);
            log.info("Loading snapshot file {}", snapshotFile);
            state.putAll(JavaUtils.cast(in.readObject()));
            log.info("State is now: {}", state);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load " + snapshot, e);
        }

        return last.getIndex();
    }

    @Override
    public long takeSnapshot() {
        final Map<EntityType, Map<String, Object>> copy;
        final TermIndex last;

        try (AutoCloseableLock lock = readLock()) {
            copy = new HashMap<>(state);
            last = getLastAppliedTermIndex();
        }

        final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
        log.info("Taking a snapshot to file {}", snapshotFile);

        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(FileUtils.newOutputStream(snapshotFile)))) {
            out.writeObject(copy);
        } catch(IOException ioe) {
            log.error("Failed to write snapshot file \"" + snapshotFile + "\", last applied index=" + last, ioe);
        }

        final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
        final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
        storage.updateLatestSnapshot(new SingleFileSnapshotInfo(info, last));
        return last.getIndex();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        final String messageString = entry.getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
        final String[] messageParts = messageString.split(" ");
        final RequestType requestType = RequestType.valueOf(messageParts[0]);

        return switch (requestType) {
            case TABLE_CREATE -> {
                TableCreateRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("%s request received. Table = '%s'.".formatted(RequestType.TABLE_CREATE, request.getTableName()));

                try (AutoCloseableLock lock = writeLock()) {
                    boolean tableExists = state.get(EntityType.TABLE).containsKey(request.getTableName());
                    if (tableExists) {
                        String errorMessage = "Table '%s' exists.".formatted(request.getTableName());
                        log.warn("{} failed. {}.", RequestType.TABLE_CREATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.CONFLICT.value(), errorMessage));
                    }

                    List<StorageNode> activeStorageNodes = state.get(EntityType.STORAGE_NODE)
                            .values()
                            .stream()
                            .map(StorageNode.class::cast)
                            .filter(StorageNode::isActive)
                            .toList();

                    if (activeStorageNodes.size() < request.getReplicationFactor()) {
                        String errorMessage = "Replication factor '%s' exceeds the number of available storage nodes '%s'."
                                .formatted(request.getReplicationFactor(), activeStorageNodes.size());
                        log.error("{} failed for table '{}'. {}", RequestType.TABLE_CREATE, request.getTableName(), errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value(), errorMessage));
                    }

                    Table table = Table.toTable(request);
                    ReplicaAssigner replicaAssigner = new ReplicaAssigner();
                    replicaAssigner.assign(table, activeStorageNodes);
                    state.get(EntityType.TABLE).put(request.getTableName(), table);
                    incrementVersion();
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.info("{} succeeded. Table = '{}'.", RequestType.TABLE_CREATE, request.getTableName());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_JOIN -> {
                StorageNodeJoinRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("{} request received. Node = '{}'.", RequestType.STORAGE_NODE_JOIN, request.getId());

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (nodeExists) {
                        StorageNode existingNodeInfo = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());

                        if (!(request.getHost().equals(existingNodeInfo.getHost()) &&
                            request.getPort() == existingNodeInfo.getPort())) {
                            log.warn("{} failed. {}.", RequestType.STORAGE_NODE_JOIN, "Node info does not match what is registered.");
                            yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value()));
                        }

                        if (existingNodeInfo.isActive()) {
                            String errorMessage = "Node '%s' is already registered.".formatted(request.getId());
                            log.warn("{} failed. {}.", RequestType.STORAGE_NODE_JOIN, errorMessage);
                            yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.CONFLICT.value(), errorMessage));
                        }

                        String infoMessage = "Node '%s' has re-joined the cluster.".formatted(request.getId());
                        log.info("{} succeeded. {}", RequestType.STORAGE_NODE_JOIN, infoMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
                    } else {
                        StorageNode  storageNode = StorageNode.toStorageNode(request);
                        state.get(EntityType.STORAGE_NODE).put(storageNode.getId(), storageNode);
                        incrementVersion();
                        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());

                        String infoMessage = "Node '%s' has joined the cluster.".formatted(storageNode.getId());
                        log.info("{} succeeded. {}", RequestType.STORAGE_NODE_JOIN, infoMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value(), infoMessage));
                    }
                }
            }
            case STORAGE_NODE_LEAVE -> {
                StorageNodeLeaveRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("{} request received. Node = '{}'.", RequestType.STORAGE_NODE_JOIN, request.getId());

                // TODO
                // a lot more needs to be done when a storage node leaves.
                // ******************************************************
                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", RequestType.STORAGE_NODE_LEAVE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    state.get(EntityType.STORAGE_NODE).remove(request.getId());
                    incrementVersion();
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.info("{} succeeded. Node = '{}'.", RequestType.STORAGE_NODE_LEAVE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_HEARTBEAT -> {
                StorageNodeHeartbeatRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", RequestType.STORAGE_NODE_HEARTBEAT, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }
                    StorageNode storageNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());

                    long currentMetadataVersion = (Long)state.get(EntityType.VERSION).get(CURRENT);

                    log.debug("{} succeeded. Node: '{}', node metadata version: {}, current version: {}.",
                            RequestType.STORAGE_NODE_HEARTBEAT,
                            request.getId(),
                            request.getLastFetchedMetadataVersion(),
                            currentMetadataVersion);

                    boolean isLaggingOnMetadata = request.getLastFetchedMetadataVersion() < currentMetadataVersion;
                    StorageNodeHeartbeatResponse response = new StorageNodeHeartbeatResponse(isLaggingOnMetadata);

                    // TODO - get it from config
                    int threshold = 3;
                    if (request.getLastFetchedMetadataVersion() < currentMetadataVersion - threshold) {
                        log.info("Node '{}' is lagging behind on metadata beyond the threshold '{}'. Marking it as inactive, " +
                                        "moving owned partitions, and removing it from ISR lists.",
                                request.getId(),
                                threshold
                        );
                        // TODO
                        // move owned partitions
                        // remove from ISR lists.
                        storageNode.setState(StorageNodeState.INACTIVE);
                    }

                    storageNode.setLastHeartbeatReceivedTime(System.nanoTime());
                    incrementVersion();
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                    yield CompletableFuture.completedFuture(response);
                }
            }
            case STORAGE_NODE_STATUS_UPDATE -> {
                System.out.println("STORAGE_NODE_STATUS_UPDATE ... more to come.");
                yield CompletableFuture.completedFuture(GenericResponse.NONE);
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value()));
        };
    }

    @Override
    public CompletableFuture<Message> query(Message message) {
        final String messageString = message.getContent().toString(StandardCharsets.UTF_8);
        final String[] messageParts = messageString.split(" ");
        final RequestType requestType = RequestType.valueOf(messageParts[0]);

        return switch (requestType) {
            case TABLE_GET -> {
                TableGetRequest request = JavaSerDe.deserialize(messageParts[1]);

                Table result;
                try (AutoCloseableLock lock = readLock()) {
                    result = (Table)state.get(EntityType.TABLE).getOrDefault(request.getTableName(), null);
                    if (result != null) {
                        result = result.copy();
                    }
                }

                log.debug("{}: {} = {}", RequestType.TABLE_GET, request.getTableName(), result);

                if (result == null) {
                    yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.NOT_FOUND.value()));
                }

                TableGetResponse response = new TableGetResponse(result);
                yield CompletableFuture.completedFuture(response);
            }
            case TABLE_LIST -> {
                // TODO - consider limit and continuation
                final List<Table> result;
                try (AutoCloseableLock lock = readLock()) {
                    result = new ArrayList<>(state.get(EntityType.TABLE).size());
                    result.addAll(state.get(EntityType.TABLE)
                            .values()
                            .stream()
                            .map(Table.class::cast)
                            .map(Table::copy)
                            .toList());
                }
                log.debug("{}: = {}", RequestType.TABLE_LIST, result);
                yield CompletableFuture.completedFuture(new TableListResponse(result));
            }
            case STORAGE_NODE_GET -> {
                StorageNodeGetRequest request = JavaSerDe.deserialize(messageParts[1]);

                log.error("Config: {}", state.get(EntityType.CONFIGURATION));

                StorageNode result;
                try (AutoCloseableLock lock = readLock()) {
                    result = (StorageNode)state.get(EntityType.STORAGE_NODE).getOrDefault(request.getId(), null);
                    if (result != null) {
                        result = result.copy();
                    }
                }

                log.debug("{}: {} = {}", RequestType.STORAGE_NODE_GET, request.getId(), result);

                if (result == null) {
                    yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.NOT_FOUND.value()));
                }

                StorageNodeGetResponse response = new StorageNodeGetResponse(result);
                yield CompletableFuture.completedFuture(response);
            }
            case STORAGE_NODE_LIST -> {
                // TODO - consider limit and continuation
                final List<StorageNode> result;
                try (AutoCloseableLock lock = readLock()) {
                    result = new ArrayList<>(state.get(EntityType.STORAGE_NODE).size());
                    result.addAll(state.get(EntityType.STORAGE_NODE)
                            .values()
                            .stream()
                            .map(StorageNode.class::cast)
                            .map(StorageNode::copy)
                            .toList());
                }
                log.debug("{}: = {}", RequestType.STORAGE_NODE_LIST, result);
                yield CompletableFuture.completedFuture(new StorageNodeListResponse(result));
            }
            case METADATA_REFRESH -> {
                MetadataRefreshRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = readLock()) {
                    log.debug("{}: client id = {}, last fetched version = {}, current version = {}",
                            RequestType.METADATA_REFRESH,
                            request.getClientId(),
                            request.getLastFetchedVersion(),
                            state.get(EntityType.VERSION).get(CURRENT));

                    if (!state.get(EntityType.VERSION).get(CURRENT).equals(request.getLastFetchedVersion())) {
                        // TODO - work on sending incremental updates - using WAL?
                        // TODO - do we really need to copy it as long as it's serialized in the lock block?
                        Map<EntityType, Map<String, Object>> stateCopy = state;
                        yield  CompletableFuture.completedFuture(new MetadataRefreshResponse(
                                stateCopy,
                                (Long)state.get(EntityType.VERSION).get(CURRENT)));
                    } else {
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.NOT_MODIFIED.value()));
                    }
                }
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value()));
        };
    }
}

