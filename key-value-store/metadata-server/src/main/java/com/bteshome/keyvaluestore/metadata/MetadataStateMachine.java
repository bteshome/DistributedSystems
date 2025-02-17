package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.*;
import com.bteshome.keyvaluestore.common.requests.*;
import com.bteshome.keyvaluestore.common.responses.*;
import com.bteshome.keyvaluestore.common.requests.MetadataRequestType;
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
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class MetadataStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<EntityType, Map<String, Object>> state;
    private final MetadataSettings metadataSettings;
    private final MetadataClientSettings metadataClientSettings;
    private ScheduledExecutorService heartBeatMonitorExecutor = null;
    private static final String CURRENT = "current";

    public MetadataStateMachine(MetadataSettings metadataSettings, MetadataClientSettings metadataClientSettings) {
        this.metadataSettings = metadataSettings;
        this.metadataClientSettings = metadataClientSettings;
        this.state = new ConcurrentHashMap<>();
        state.put(EntityType.TABLE, new ConcurrentHashMap<>());
        state.put(EntityType.STORAGE_NODE, new ConcurrentHashMap<>());
        state.put(EntityType.CONFIGURATION, new ConcurrentHashMap<>());
        state.put(EntityType.VERSION, new ConcurrentHashMap<>());
        state.get(EntityType.VERSION).put(CURRENT, 0L);
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
            if (heartBeatMonitorExecutor != null) {
                heartBeatMonitorExecutor.close();
                log.info("Stopped storage node heartbeat monitor.");
            }
        }
    }

    @Override
    public LeaderEventApi leaderEvent() {
        try (AutoCloseableLock l = readLock()) {
            if (state.get(EntityType.CONFIGURATION).isEmpty())
                ConfigurationLoader.load(state, metadataSettings);
            loadUnmanagedState();
            scheduleStorageMonitor();
            return super.leaderEvent();
        }
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
        if (md5 != null)
            MD5FileUtil.verifySavedMD5(snapshotFile, md5);

        final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
        try (AutoCloseableLock lock = writeLock();
             ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(FileUtils.newInputStream(snapshotFile)))) {
            reset();
            setLastAppliedTermIndex(last);
            log.info("Loading snapshot file {}", snapshotFile);
            state.putAll(JavaUtils.cast(in.readObject()));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load " + snapshot, e);
        }

        return last.getIndex();
    }

    @Override
    public long takeSnapshot() {
        final TermIndex last;
        final File snapshotFile;

        try (AutoCloseableLock lock = readLock()) {
            last = getLastAppliedTermIndex();
            snapshotFile = storage.getSnapshotFile(last.getTerm(), last.getIndex());
            log.info("Taking a snapshot to file {}", snapshotFile);
            try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(FileUtils.newOutputStream(snapshotFile)))) {
                out.writeObject(state);
            } catch (IOException ioe) {
                log.error("Failed to write snapshot file \"" + snapshotFile + "\", last applied index=" + last, ioe);
            }
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
        final MetadataRequestType requestType = MetadataRequestType.valueOf(messageParts[0]);

        return switch (requestType) {
            case TABLE_CREATE -> {
                TableCreateRequest request = JavaSerDe.deserialize(messageParts[1]);
                List<StorageNode> activeStorageNodes;
                log.info("%s request received. Table = '%s'.".formatted(MetadataRequestType.TABLE_CREATE, request.getTableName()));

                try (AutoCloseableLock lock = writeLock()) {
                    boolean tableExists = state.get(EntityType.TABLE).containsKey(request.getTableName());
                    if (tableExists) {
                        String errorMessage = "Table '%s' exists.".formatted(request.getTableName());
                        log.warn("{} failed. {}.", MetadataRequestType.TABLE_CREATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.CONFLICT.value(), errorMessage));
                    }

                    activeStorageNodes = state.get(EntityType.STORAGE_NODE)
                            .values()
                            .stream()
                            .map(StorageNode.class::cast)
                            .filter(StorageNode::isActive)
                            .toList();

                    if (activeStorageNodes.size() < request.getReplicationFactor()) {
                        String errorMessage = "Replication factor '%s' exceeds the number of available storage nodes '%s'."
                                .formatted(request.getReplicationFactor(), activeStorageNodes.size());
                        log.error("{} failed for table '{}'. {}", MetadataRequestType.TABLE_CREATE, request.getTableName(), errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value(), errorMessage));
                    }

                    Table table = Table.toTable(request);
                    ReplicaAssigner.assign(table, activeStorageNodes);
                    PartitionLeaderElector.elect(table, state.get(EntityType.STORAGE_NODE));
                    state.get(EntityType.TABLE).put(request.getTableName(), table);
                    incrementVersion(entry);

                    if (trx.getServerRole().equals(RaftProtos.RaftPeerRole.LEADER))
                        sendTableCreatedNotification(table);
                }

                log.info("{} succeeded. Table = '{}'.", MetadataRequestType.TABLE_CREATE, request.getTableName());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_JOIN -> {
                StorageNodeJoinRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("{} request received. Node = '{}'.", MetadataRequestType.STORAGE_NODE_JOIN, request.getId());

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (nodeExists) {
                        StorageNode existingNodeInfo = (StorageNode) state.get(EntityType.STORAGE_NODE).get(request.getId());

                        if (!(request.getHost().equals(existingNodeInfo.getHost()) && request.getPort() == existingNodeInfo.getPort())) {
                            log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_JOIN, "Node info does not match what is registered.");
                            yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value()));
                        }

                        switch (existingNodeInfo.getStatus()) {
                            case ACTIVE -> {
                                log.warn("{}: {}.", MetadataRequestType.STORAGE_NODE_JOIN, "Node is already active.");
                                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
                            }
                            case INACTIVE -> {
                                String infoMessage = "Node '%s' has rejoined the cluster.".formatted(request.getId());
                                log.info("{} succeeded. {}", MetadataRequestType.STORAGE_NODE_JOIN, infoMessage);
                                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value(), infoMessage));
                            }
                            case REMOVED -> {
                                log.warn("{}: {}.", MetadataRequestType.STORAGE_NODE_JOIN, "Node is already removed.");
                                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value()));
                            }
                        }
                    }

                    StorageNode storageNode = StorageNode.toStorageNode(request);
                    state.get(EntityType.STORAGE_NODE).put(storageNode.getId(), storageNode);
                    UnmanagedState.getInstance().addStorageNode(new UnmanagedState.StorageNode(storageNode.getId(), storageNode.getStatus()));
                    incrementVersion(entry);
                }

                String infoMessage = "Node '%s' has joined the cluster.".formatted(request.getId());
                log.info("{} succeeded. {}", MetadataRequestType.STORAGE_NODE_JOIN, infoMessage);
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value(), infoMessage));
            }
            case STORAGE_NODE_LEAVE -> {
                StorageNodeLeaveRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("{} request received. Node = '{}'.", MetadataRequestType.STORAGE_NODE_JOIN, request.getId());

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_LEAVE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    StorageNode storageNode = (StorageNode) state.get(EntityType.STORAGE_NODE).get(request.getId());

                    if (storageNode.getStatus().equals(StorageNodeStatus.REMOVED)) {
                        String errorMessage = "Node '%s' is already marked as removed.".formatted(request.getId());
                        log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_LEAVE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value(), errorMessage));
                    }

                    storageNode.setStatus(StorageNodeStatus.REMOVED);
                    UnmanagedState.getInstance().setStorageNodeStatus(request.getId(), StorageNodeStatus.REMOVED);
                    List<Partition> affectedPartitions = PartitionLeaderElector.oustAndReelect(storageNode,
                            state.get(EntityType.TABLE),
                            state.get(EntityType.STORAGE_NODE));
                    incrementVersion(entry);

                    if (trx.getServerRole().equals(RaftProtos.RaftPeerRole.LEADER))
                        sendNewLeaderElectedNotification(affectedPartitions);
                }

                log.info("{} succeeded. Node = '{}'.", MetadataRequestType.STORAGE_NODE_LEAVE, request.getId());
                String infoMessage = "Node '%s' has left the cluster. Moving the replicas is pending".formatted(request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value(), infoMessage));
            }
            case STORAGE_NODE_ACTIVATE -> {
                StorageNodeActivateRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_ACTIVATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    StorageNode storageNode = (StorageNode) state.get(EntityType.STORAGE_NODE).get(request.getId());
                    storageNode.setStatus(StorageNodeStatus.ACTIVE);
                    UnmanagedState.getInstance().setStorageNodeStatus(request.getId(), StorageNodeStatus.ACTIVE);
                    incrementVersion(entry);
                }

                log.info("{} succeeded. Node = '{}'.", MetadataRequestType.STORAGE_NODE_ACTIVATE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case STORAGE_NODE_DEACTIVATE -> {
                StorageNodeDeactivateRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.warn("{} request received. Node = '{}'.", MetadataRequestType.STORAGE_NODE_DEACTIVATE, request.getId());

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' is unrecognized.".formatted(request.getId());
                        log.warn("{} failed. {}.", MetadataRequestType.STORAGE_NODE_DEACTIVATE, errorMessage);
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.UNAUTHORIZED.value(), errorMessage));
                    }

                    StorageNode storageNode = (StorageNode) state.get(EntityType.STORAGE_NODE).get(request.getId());
                    storageNode.setStatus(StorageNodeStatus.INACTIVE);
                    UnmanagedState.getInstance().setStorageNodeStatus(request.getId(), StorageNodeStatus.INACTIVE);
                    List<Partition> affectedPartitions = PartitionLeaderElector.oustAndReelect(storageNode,
                            state.get(EntityType.TABLE),
                            state.get(EntityType.STORAGE_NODE));
                    incrementVersion(entry);

                    if (trx.getServerRole().equals(RaftProtos.RaftPeerRole.LEADER))
                        sendNewLeaderElectedNotification(affectedPartitions);
                }

                log.info("{} succeeded. Node = '{}'.", MetadataRequestType.STORAGE_NODE_DEACTIVATE, request.getId());
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case REPLICA_REMOVE_FROM_ISR -> {
                ReplicaRemoveFromISRRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.warn("{} request received. Number of replicas = '{}'.", MetadataRequestType.REPLICA_REMOVE_FROM_ISR, request.getReplicas().size());

                try (AutoCloseableLock lock = writeLock()) {
                    for (Replica replica : request.getReplicas()) {
                        Table table = (Table) state.get(EntityType.TABLE).get(replica.getTable());
                        Partition partition = table.getPartitions().get(replica.getPartition());
                        partition.getInSyncReplicas().remove(replica.getNodeId());
                        log.warn("'{}' removed from the ISR list of table '{}' partition '{}'.",
                                replica.getNodeId(),
                                replica.getTable(),
                                replica.getPartition());
                    }

                    incrementVersion(entry);

                    if (trx.getServerRole().equals(RaftProtos.RaftPeerRole.LEADER))
                        sendISRListChangedNotification(request.getReplicas());
                }

                log.warn("{} succeeded.", MetadataRequestType.REPLICA_REMOVE_FROM_ISR);
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            case REPLICA_ADD_TO_ISR -> {
                ReplicaAddToISRRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.warn("{} request received. Number of replicas = '{}'.", MetadataRequestType.REPLICA_ADD_TO_ISR, request.getReplicas().size());

                try (AutoCloseableLock lock = writeLock()) {
                    for (Replica replica : request.getReplicas()) {
                        Table table = (Table) state.get(EntityType.TABLE).get(replica.getTable());
                        Partition partition = table.getPartitions().get(replica.getPartition());
                        partition.getInSyncReplicas().add(replica.getNodeId());
                        log.warn("'{}' added to the ISR list of table '{}' partition '{}'.",
                                replica.getNodeId(),
                                replica.getTable(),
                                replica.getPartition());
                    }

                    incrementVersion(entry);

                    if (trx.getServerRole().equals(RaftProtos.RaftPeerRole.LEADER))
                        sendISRListChangedNotification(request.getReplicas());
                }

                log.warn("{} succeeded.", MetadataRequestType.REPLICA_ADD_TO_ISR);
                yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.OK.value()));
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value()));
        };
    }

    @Override
    public CompletableFuture<Message> query(Message message) {
        final String messageString = message.getContent().toString(StandardCharsets.UTF_8);
        final String[] messageParts = messageString.split(" ");
        final MetadataRequestType requestType = MetadataRequestType.valueOf(messageParts[0]);

        return switch (requestType) {
            case TABLE_GET -> {
                TableGetRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = readLock()) {
                    Table result = (Table) state.get(EntityType.TABLE).getOrDefault(request.getTableName(), null);

                    log.debug("{}: {} = {}", MetadataRequestType.TABLE_GET, request.getTableName(), result);

                    if (result == null)
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.NOT_FOUND.value()));

                    TableGetResponse response = new TableGetResponse(result);
                    yield CompletableFuture.completedFuture(response);
                }
            }
            case TABLE_LIST -> {
                // TODO - consider limit and continuation
                try (AutoCloseableLock lock = readLock()) {
                    List<Table> result = new ArrayList<>(state.get(EntityType.TABLE)
                            .values()
                            .stream()
                            .map(Table.class::cast)
                            .toList());
                    log.debug("{}: = {}", MetadataRequestType.TABLE_LIST, result);
                    yield CompletableFuture.completedFuture(new TableListResponse(result));
                }
            }
            case STORAGE_NODE_GET -> {
                StorageNodeGetRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = readLock()) {
                    StorageNode result = (StorageNode) state.get(EntityType.STORAGE_NODE).getOrDefault(request.getId(), null);
                    log.debug("{}: {} = {}", MetadataRequestType.STORAGE_NODE_GET, request.getId(), result);
                    if (result == null)
                        yield CompletableFuture.completedFuture(new GenericResponse(HttpStatus.NOT_FOUND.value()));
                    StorageNodeGetResponse response = new StorageNodeGetResponse(result);
                    yield CompletableFuture.completedFuture(response);
                }
            }
            case STORAGE_NODE_LIST -> {
                // TODO - consider limit and continuation
                try (AutoCloseableLock lock = readLock()) {
                    List<StorageNode> result = new ArrayList<>(state.get(EntityType.STORAGE_NODE)
                            .values()
                            .stream()
                            .map(StorageNode.class::cast)
                            .toList());
                    log.debug("{}: = {}", MetadataRequestType.STORAGE_NODE_LIST, result);
                    yield CompletableFuture.completedFuture(new StorageNodeListResponse(result));
                }
            }
            case STORAGE_NODE_METADATA_REFRESH -> {
                StorageNodeMetadataRefreshRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = readLock()) {
                    long currentVersion = (Long)state.get(EntityType.VERSION).get(CURRENT);

                    log.debug("{}: node = {}, last fetched version = {}, current version = {}",
                            MetadataRequestType.STORAGE_NODE_METADATA_REFRESH,
                            request.getId(),
                            request.getLastFetchedVersion(),
                            currentVersion);

                    String heartbeatEndpoint = "%s:%s/api/heartbeat/".formatted(
                            metadataSettings.getNode().getHost(),
                            metadataSettings.getRestPort());

                    if (request.getLastFetchedVersion() == 0 || request.getLastFetchedVersion() != currentVersion) {
                        UnmanagedState.getInstance().setMetadataFetchTime(request.getId(), System.nanoTime());
                        yield CompletableFuture.completedFuture(new StorageNodeMetadataRefreshResponse(state, heartbeatEndpoint, true));
                    } else {
                        yield CompletableFuture.completedFuture(new StorageNodeMetadataRefreshResponse(null, heartbeatEndpoint, false));
                    }
                }
            }
            case CLIENT_METADATA_REFRESH -> {
                ClientMetadataRefreshRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = readLock()) {
                    long currentVersion = (Long) state.get(EntityType.VERSION).get(CURRENT);

                    if (request.getLastFetchedVersion() == 0 || request.getLastFetchedVersion() != currentVersion)
                        yield CompletableFuture.completedFuture(new ClientMetadataRefreshResponse(state, true));
                    else
                        yield CompletableFuture.completedFuture(new ClientMetadataRefreshResponse(null, false));
                }
            }
            case CONFIGURATION_LIST -> {
                try (AutoCloseableLock lock = readLock()) {
                    Map<String, Object> result = state.get(EntityType.CONFIGURATION);
                    yield CompletableFuture.completedFuture(new ConfigurationListResponse(result));
                }
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(HttpStatus.BAD_REQUEST.value()));
        };
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    private void incrementVersion(RaftProtos.LogEntryProto entry) {
        long version = (Long) state.get(EntityType.VERSION).get(CURRENT);
        version++;
        state.get(EntityType.VERSION).put(CURRENT, version);
        UnmanagedState.getInstance().setVersion(version);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    }

    private void scheduleStorageMonitor() {
        long monitorIntervalMs = (Long) state.get(EntityType.CONFIGURATION).get(ConfigKeys.STORAGE_NODE_HEARTBEAT_MONITOR_INTERVAL_MS_KEY);
        try {
            heartBeatMonitorExecutor = Executors.newSingleThreadScheduledExecutor();
            heartBeatMonitorExecutor.scheduleAtFixedRate(
                    () -> new StorageNodeMonitor().checkStatus(metadataClientSettings),
                    monitorIntervalMs,
                    monitorIntervalMs,
                    TimeUnit.MILLISECONDS);
            log.info("Scheduled storage node monitor. The interval is {} ms.", monitorIntervalMs);
        } catch (Exception e) {
            log.error("Error scheduling storage node monitor: ", e);
        }
    }

    private void reset() {
        state.get(EntityType.TABLE).clear();
        state.get(EntityType.STORAGE_NODE).clear();
        state.get(EntityType.CONFIGURATION).clear();
        state.get(EntityType.VERSION).put(CURRENT, 0L);
        setLastAppliedTermIndex(null);
    }

    private void loadUnmanagedState() {
        List<UnmanagedState.StorageNode> unmanagedStorageNodes = state.get(EntityType.STORAGE_NODE)
                .values()
                .stream()
                .map(StorageNode.class::cast)
                .map(node -> new UnmanagedState.StorageNode(node.getId(), node.getStatus()))
                .toList();
        UnmanagedState.getInstance().setStorageNodes(unmanagedStorageNodes);
        UnmanagedState.getInstance().setConfiguration(state.get(EntityType.CONFIGURATION));
        UnmanagedState.getInstance().setVersion((Long) state.get(EntityType.VERSION).get(CURRENT));
        UnmanagedState.getInstance().setLeader();
    }

    private void sendTableCreatedNotification(Table table) {
        Set<String> replicaEndpoints = state.get(EntityType.STORAGE_NODE)
                .values()
                .stream()
                .map(StorageNode.class::cast)
                .filter(node -> node.hasReplicasFor(table.getName()))
                .map(node -> "%s:%s".formatted(node.getHost(), node.getPort()))
                .collect(Collectors.toSet());
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        replicaEndpoints.forEach(endpoint -> {
            futures.add(CompletableFuture.runAsync(() -> {
                RestClient.builder()
                        .build()
                        .post()
                        .uri("http://%s/api/metadata/table-created/".formatted(endpoint))
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(table)
                        .retrieve()
                        .toBodilessEntity();
            }));
        });

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        log.info("Sent table created message to all replicas of all partitions for table '{}'..", table.getName());
    }

    private void sendNewLeaderElectedNotification(List<Partition> affectedPartitions) {
        affectedPartitions.forEach(partition -> {
            NewLeaderElectedRequest newLeaderElectedRequest = new NewLeaderElectedRequest(
                    partition.getTableName(),
                    partition.getId(),
                    partition.getLeader(),
                    partition.getLeaderTerm(),
                    partition.getInSyncReplicas());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            List<String> replicaEndpoints = partition
                    .getReplicas()
                    .stream()
                    .map(replicaNodeId -> {
                        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(replicaNodeId);
                        return "%s:%s".formatted(node.getHost(), node.getPort());
                    })
                    .toList();

            replicaEndpoints.forEach(endpoint -> {
                futures.add(CompletableFuture.runAsync(() -> {
                    RestClient.builder()
                            .build()
                            .post()
                            .uri("http://%s/api/metadata/new-leader-elected/".formatted(endpoint))
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(newLeaderElectedRequest)
                            .retrieve()
                            .toBodilessEntity();
                }));
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        });

        log.info("Sent new leader elected message to all replicas of affected partitions '{}'.", affectedPartitions);
    }

    private void sendISRListChangedNotification(Set<Replica> affectedReplicas) {
        Set<Partition> affectedPartitions = affectedReplicas.stream()
                .map(replica -> {
                    Table table = (Table) state.get(EntityType.TABLE).get(replica.getTable());
                    return table.getPartitions().get(replica.getPartition());
                })
                .collect(Collectors.toSet());

        affectedPartitions.forEach(partition -> {
            ISRListChangedRequest isrListChangedRequest = new ISRListChangedRequest(
                    partition.getTableName(),
                    partition.getId(),
                    partition.getInSyncReplicas());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            List<String> replicaEndpoints = partition
                    .getReplicas()
                    .stream()
                    .map(replicaNodeId -> {
                        StorageNode node = (StorageNode)state.get(EntityType.STORAGE_NODE).get(replicaNodeId);
                        return "%s:%s".formatted(node.getHost(), node.getPort());
                    })
                    .toList();

            replicaEndpoints.forEach(endpoint -> {
                futures.add(CompletableFuture.runAsync(() -> {
                    RestClient.builder()
                            .build()
                            .post()
                            .uri("http://%s/api/metadata/isr-list-changed/".formatted(endpoint))
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(isrListChangedRequest)
                            .retrieve()
                            .toBodilessEntity();
                }));
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        });

        log.info("Sent ISR list changed notification to all replicas of affected partitions '{}'.", affectedPartitions);
    }
}

