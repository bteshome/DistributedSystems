package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.StorageNodeStatus;
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
    private ScheduledExecutorService heartBeatMonitorExecutor = null;
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

    private void incrementVersion(RaftProtos.LogEntryProto entry) {
        long version = Long.parseLong(state.get(EntityType.VERSION).get(CURRENT).toString());
        version++;
        state.get(EntityType.VERSION).put(CURRENT, version);
        UnmanagedState.getInstance().setVersion(version);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    }

    private void scheduleStorageNodeHeartbeatMonitor() {
        long monitorIntervalMs = (Long)state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_HEARTBEAT_MONITOR_INTERVAL_MS_KEY);
        try {
            heartBeatMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            heartBeatMonitorExecutor.scheduleAtFixedRate(
                    () -> new StorageNodeHeartbeatMonitor().checkStatus(metadataSettings),
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
        if (heartBeatMonitorExecutor != null) {
            heartBeatMonitorExecutor.close();
        }
        super.close();
    }

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        super.notifyLeaderChanged(groupMemberId, newLeaderId);
        if (!newLeaderId.equals(groupMemberId.getPeerId())) {
            UnmanagedState.getInstance().clear();
            if (heartBeatMonitorExecutor != null) heartBeatMonitorExecutor.close();
            log.info("Stopped storage node heartbeat monitor");
        }
    }

    @Override
    public LeaderEventApi leaderEvent() {
        if (state.get(EntityType.CONFIGURATION).isEmpty()) {
            try (AutoCloseableLock lock = writeLock()) {
                new ConfigurationLoader().load(state, metadataSettings);
            }
        }
        loadUnmanagedState();
        scheduleStorageNodeHeartbeatMonitor();
        return super.leaderEvent();
    }

    private void loadUnmanagedState() {
        UnmanagedState.getInstance().setStorageNodes(state.get(EntityType.STORAGE_NODE)
                .values()
                .stream()
                .map(StorageNode.class::cast)
                .toList());
        UnmanagedState.getInstance().setConfiguration(state.get(EntityType.CONFIGURATION));
        UnmanagedState.getInstance().setVersion((Long)state.get(EntityType.VERSION).get(CURRENT));
        UnmanagedState.getInstance().setLeader();
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
                List<StorageNode> activeStorageNodes;
                log.info("%s request received. Table = '%s'.".formatted(RequestType.TABLE_CREATE, request.getTableName()));

                try (AutoCloseableLock lock = readLock()) {
                    boolean tableExists = state.get(EntityType.TABLE).containsKey(request.getTableName());
                    if (tableExists) {
                        String errorMessage = "Table '%s' exists.".formatted(request.getTableName());
                        log.warn("{} failed. {}.", RequestType.TABLE_CREATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.CONFLICT.value(), errorMessage));
                    }

                    activeStorageNodes = state.get(EntityType.STORAGE_NODE)
                            .values()
                            .stream()
                            .map(StorageNode.class::cast)
                            .filter(StorageNode::isActive)
                            .toList();
                }

                if (activeStorageNodes.size() < request.getReplicationFactor()) {
                    String errorMessage = "Replication factor '%s' exceeds the number of available storage nodes '%s'."
                            .formatted(request.getReplicationFactor(), activeStorageNodes.size());
                    log.error("{} failed for table '{}'. {}", RequestType.TABLE_CREATE, request.getTableName(), errorMessage);
                    yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value(), errorMessage));
                }

                Table table = Table.toTable(request);

                try (AutoCloseableLock lock = writeLock()) {
                    ReplicaAssigner.assign(table, activeStorageNodes);
                    PartitionLeaderElector.elect(table, state.get(EntityType.STORAGE_NODE));
                    state.get(EntityType.TABLE).put(request.getTableName(), table);
                    incrementVersion(entry);
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
                    }

                    StorageNode storageNode = StorageNode.toStorageNode(request);
                    state.get(EntityType.STORAGE_NODE).put(storageNode.getId(), storageNode);
                    UnmanagedState.getInstance().addStorageNode(storageNode);
                    incrementVersion(entry);

                    String infoMessage = "Node '%s' has joined the cluster.".formatted(storageNode.getId());
                    log.info("{} succeeded. {}", RequestType.STORAGE_NODE_JOIN, infoMessage);
                    yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value(), infoMessage));
                }
            }
            case STORAGE_NODE_LEAVE -> {
                StorageNodeLeaveRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("{} request received. Node = '{}'.", RequestType.STORAGE_NODE_JOIN, request.getId());

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", RequestType.STORAGE_NODE_LEAVE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    StorageNode storageNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());
                    PartitionLeaderElector.oustAndReelect(storageNode, state.get(EntityType.TABLE), state.get(EntityType.STORAGE_NODE));
                    state.get(EntityType.STORAGE_NODE).remove(request.getId());
                    UnmanagedState.getInstance().removeStorageNode(request.getId());
                    incrementVersion(entry);
                }

                log.info("{} succeeded. Node = '{}'.", RequestType.STORAGE_NODE_LEAVE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_ACTIVATE -> {
                StorageNodeActivateRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", RequestType.STORAGE_NODE_ACTIVATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    StorageNode storageNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());
                    // TODO - add it back to the ISR lists -
                    //        also, what is the partition leader's role here?
                    storageNode.setStatus(StorageNodeStatus.ACTIVE);
                    UnmanagedState.getInstance().setStorageNodeStatus(request.getId(), StorageNodeStatus.ACTIVE);
                    incrementVersion(entry);
                }

                log.info("{} succeeded. Node = '{}'.", RequestType.STORAGE_NODE_ACTIVATE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_DEACTIVATE -> {
                StorageNodeDeactivateRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", RequestType.STORAGE_NODE_DEACTIVATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    // TODO - what is the partition leader's role here?
                    StorageNode storageNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());
                    storageNode.setStatus(StorageNodeStatus.INACTIVE);
                    UnmanagedState.getInstance().setStorageNodeStatus(request.getId(), StorageNodeStatus.INACTIVE);
                    PartitionLeaderElector.oustAndReelect(storageNode, state.get(EntityType.TABLE), state.get(EntityType.STORAGE_NODE));
                    incrementVersion(entry);
                }

                log.info("{} succeeded. Node = '{}'.", RequestType.STORAGE_NODE_DEACTIVATE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
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
                long currentVersion;

                try (AutoCloseableLock lock = readLock()) {
                    currentVersion = (Long)state.get(EntityType.VERSION).get(CURRENT);
                }

                log.debug("{}: client id = {}, last fetched version = {}, current version = {}",
                        RequestType.METADATA_REFRESH,
                        request.getClientId(),
                        request.getLastFetchedVersion(),
                        currentVersion);

                String heartbeatEndpoint = "%s:%s/api/heartbeat/".formatted(
                        metadataSettings.getNode().get("host"),
                        metadataSettings.getRestPort());

                if (currentVersion == request.getLastFetchedVersion()) {
                    yield CompletableFuture.completedFuture(new MetadataRefreshResponse(heartbeatEndpoint));
                }

                try (AutoCloseableLock lock = readLock()) {
                    // TODO - work on sending incremental updates - using WAL?
                    yield  CompletableFuture.completedFuture(new MetadataRefreshResponse(state, heartbeatEndpoint));
                }
            }
            /*case STORAGE_NODE_HEARTBEAT -> {
                StorageNodeHeartbeatRequest request = JavaSerDe.deserialize(messageParts[1]);
                boolean nodeExists;
                long currentMetadataVersion ;
                long threshold;

                try (AutoCloseableLock lock = readLock()) {
                    nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    currentMetadataVersion = (Long) state.get(EntityType.VERSION).get(CURRENT);
                    threshold = (Long) state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_METADATA_LAG_MS_KEY);
                }

                if (!nodeExists) {
                    String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                    log.warn("{} failed. {}.", RequestType.STORAGE_NODE_HEARTBEAT, errorMessage);
                    yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                }

                boolean isLaggingOnMetadata = request.getLastFetchedMetadataVersion() < currentMetadataVersion;
                boolean isLaggingOnMetadataBeyondThreshold = request.getLastFetchedMetadataVersion() < (currentMetadataVersion - threshold);

                log.info("{}. Node: '{}', node metadata version: {}, current version: {}.",
                        RequestType.STORAGE_NODE_HEARTBEAT,
                        request.getId(),
                        request.getLastFetchedMetadataVersion(),
                        currentMetadataVersion);

                if (isLaggingOnMetadataBeyondThreshold) {
                    log.warn("Node '{}' is lagging on metadata beyond the threshold '{}'. Preparing to mark it as inactive.",
                            request.getId(),
                            threshold
                    );
                    StorageNodeDeactivateRequest deactivateRequest = new StorageNodeDeactivateRequest(request.getId());
                    try (RaftClient client = LocalClientBuilder.createRaftClient(metadataSettings)) {
                        final RaftClientReply reply = client.io().send(request);
                        if (reply.isSuccess()) {
                            String deactivateMessageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                            GenericResponse response = ResponseStatus.toGenericResponse(deactivateMessageString);
                            if (response.getHttpStatusCode() != HttpStatus.OK.value()) {
                                log.error(response.getMessage());
                            }
                        } else {
                            log.error("Error deactivating node '{}'.", request.getId(), reply.getException());
                        }
                    } catch (Exception e) {
                        log.error("Error deactivating node '{}'.", request.getId(), e);
                    }
                }

                try (AutoCloseableLock lock = writeLock()) {
                    UnmanagedState.heartbeats.put(request.getId(), System.nanoTime());
                }
                yield CompletableFuture.completedFuture(new StorageNodeHeartbeatResponse(isLaggingOnMetadata));
                yield CompletableFuture.completedFuture(new GenericResponse());
            }*/
            default -> CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value()));
        };
    }
}

