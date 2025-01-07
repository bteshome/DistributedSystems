package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.common.entities.StorageNode;
import com.bteshome.keyvaluestore.common.entities.StorageNodeState;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.*;
import com.bteshome.keyvaluestore.common.responses.*;
import com.bteshome.keyvaluestore.metadata.common.RequestType;
import com.bteshome.keyvaluestore.metadata.common.EntityType;
import com.bteshome.keyvaluestore.common.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MyStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<EntityType, Map<String, Object>> state;

    public MyStateMachine() {
        state = new ConcurrentHashMap<>();
        state.put(EntityType.TABLE, new HashMap<String, Object>());
        state.put(EntityType.STORAGE_NODE, new HashMap<String, Object>());
    }

    private AutoCloseableLock readLock() {
        return AutoCloseableLock.acquire(lock.readLock());
    }

    private AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry ();
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
                        log.warn("%s failed. %s.".formatted(RequestType.TABLE_CREATE, errorMessage));
                        yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.CONFLICT, errorMessage));
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
                        log.error("%s failed for table '%s'. %s".formatted(RequestType.TABLE_CREATE, request.getTableName(), errorMessage));
                        yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.BAD_REQUEST, errorMessage));
                    }

                    Table table = Table.toTable(request);
                    ReplicaAssigner replicaAssigner = new ReplicaAssigner();
                    replicaAssigner.assign(table, activeStorageNodes);
                    state.get(EntityType.TABLE).put(request.getTableName(), table);
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.info("%s succeeded. Table = '%s'.".formatted(RequestType.TABLE_CREATE, request.getTableName()));
                yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.OK));
            }
            case STORAGE_NODE_JOIN -> {
                StorageNodeJoinRequest request = JavaSerDe.deserialize(messageParts[1]);
                StorageNode storageNode = StorageNode.toStorageNode(request);
                log.info("%s request received. Node = '%s'.".formatted(RequestType.STORAGE_NODE_JOIN, request.getId()));

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(storageNode.getId());
                    if (nodeExists) {
                        String errorMessage = "Node '%s' exists.".formatted(storageNode.getId());
                        log.warn("%s failed. %s.".formatted(RequestType.STORAGE_NODE_JOIN, errorMessage));
                        yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.CONFLICT, errorMessage));
                    }

                    state.get(EntityType.STORAGE_NODE).put(storageNode.getId(), storageNode);
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                String infoMessage = "Node '%s' has joined the cluster.".formatted(storageNode.getId());
                log.info("%s succeeded. %s".formatted(RequestType.STORAGE_NODE_JOIN, infoMessage));
                yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.OK, infoMessage));
            }
            case STORAGE_NODE_LEAVE -> {
                StorageNodeLeaveRequest request = JavaSerDe.deserialize(messageParts[1]);
                log.info("%s request received. Node = '%s'.".formatted(RequestType.STORAGE_NODE_JOIN, request.getId()));

                // TODO
                // a lot more needs to be done when a storage node leaves.
                // ******************************************************
                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' does not exist.".formatted(request.getId());
                        log.warn("%s failed. %s.".formatted(RequestType.STORAGE_NODE_LEAVE, errorMessage));
                        yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.NOT_FOUND, errorMessage));
                    }

                    state.get(EntityType.STORAGE_NODE).remove(request.getId());
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.info("%s succeeded. Node = '%s'.".formatted(RequestType.STORAGE_NODE_LEAVE, request.getId()));
                yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.OK));
            }
            case STORAGE_NODE_HEARTBEAT -> {
                StorageNodeHeartbeatRequest request = JavaSerDe.deserialize(messageParts[1]);

                try (AutoCloseableLock lock = writeLock()) {
                    boolean nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(request.getId());
                    if (!nodeExists) {
                        String errorMessage = "Node '%s' does not exist.".formatted(request.getId());
                        log.warn("%s failed. %s.".formatted(RequestType.STORAGE_NODE_HEARTBEAT, errorMessage));
                        yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.NOT_FOUND, errorMessage));
                    }

                    StorageNode storageNode = (StorageNode)state.get(EntityType.STORAGE_NODE).get(request.getId());
                    storageNode.setLastHeartbeatReceivedTime(System.currentTimeMillis());
                    storageNode.setState(StorageNodeState.ACTIVE);
                    storageNode.setMetadataVersion(request.getMetadataVersion());

                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.debug("%s succeeded. Node = '%s'.".formatted(RequestType.STORAGE_NODE_HEARTBEAT, request.getId()));
                yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.OK));
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.BAD_REQUEST));
        };
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        log.info("Initialize called");
        super.initialize(raftServer, raftGroupId, storage);
        this.storage.init(storage);
        loadSnapshot(this.storage.getLatestSnapshot());
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

    private void reset() {
        state.get(EntityType.TABLE).clear();
        state.get(EntityType.STORAGE_NODE).clear();
        setLastAppliedTermIndex(null);
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
    public CompletableFuture<Message> query(Message request) {
        final String messageString = request.getContent().toString(StandardCharsets.UTF_8);
        final String[] messageParts = messageString.split(" ");
        final RequestType requestType = RequestType.valueOf(messageParts[0]);

        return switch (requestType) {
            case TABLE_GET -> {
                TableGetRequest tableGetRequest = (TableGetRequest)JavaSerDe.deserialize(messageParts[1]);

                Table result;
                try (AutoCloseableLock lock = readLock()) {
                    result = (Table)state.get(EntityType.TABLE).getOrDefault(tableGetRequest.getTableName(), null);
                    if (result != null) {
                        result = result.copy();
                    }
                }

                log.debug("TABLE_GET: {} = {}", tableGetRequest.getTableName(), result);

                if (result == null) {
                    yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.NOT_FOUND));
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
                log.debug("TABLE_LIST: = {}", result);
                yield CompletableFuture.completedFuture(new TableListResponse(result));
            }
            case STORAGE_NODE_GET -> {
                StorageNodeGetRequest storageNodeGetRequest = (StorageNodeGetRequest)JavaSerDe.deserialize(messageParts[1]);

                StorageNode result;
                try (AutoCloseableLock lock = readLock()) {
                    result = (StorageNode)state.get(EntityType.STORAGE_NODE).getOrDefault(storageNodeGetRequest.getId(), null);
                    if (result != null) {
                        result = result.copy();
                    }
                }

                log.debug("STORAGE_NODE_GET: {} = {}", storageNodeGetRequest.getId(), result);

                if (result == null) {
                    yield CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.NOT_FOUND));
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
                log.debug("STORAGE_NODE_LIST: = {}", result);
                yield CompletableFuture.completedFuture(new StorageNodeListResponse(result));
            }
            default -> CompletableFuture.completedFuture(new GenericResponse(ResponseStatus.BAD_REQUEST));
        };
    }
}

