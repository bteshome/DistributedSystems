package com.bteshome.keyvaluestore.metadata;

import com.bteshome.keyvaluestore.metadata.message.NotFoundResponse;
import com.bteshome.keyvaluestore.metadata.message.TableGetResponse;
import com.bteshome.keyvaluestore.metadata.common.RequestType;
import com.bteshome.keyvaluestore.metadata.common.EntityType;
import com.bteshome.keyvaluestore.metadata.entity.StorageNode;
import com.bteshome.keyvaluestore.metadata.entity.Table;
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
import java.util.HashMap;
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
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        final String messageString = entry.getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
        final RequestType requestType = RequestType.valueOf(messageString.split(" ")[0]);

        return switch (requestType) {
            case TABLE_CREATE -> {
                Table table = Table.toTable(messageString);

                boolean tableExists;
                try(AutoCloseableLock readLock = readLock()) {
                    tableExists = state.get(EntityType.TABLE).containsKey(table.getName());
                }

                if (tableExists) {
                    log.warn("%s failed. Table '%s' exists.".formatted(RequestType.TABLE_CREATE, table.getName()));
                    yield CompletableFuture.failedFuture(new MetadataServerException("Table exists."));
                }

                try (AutoCloseableLock writeLock = writeLock()) {
                    state.get(EntityType.TABLE).put(table.getName(), table);
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.warn("%s succeeded. Table = '%s'.".formatted(RequestType.TABLE_CREATE, table.getName()));
                yield CompletableFuture.completedFuture(Message.EMPTY);
            }
            case STORAGE_NODE_JOIN -> {
                StorageNode storageNode = StorageNode.toStorageNode(messageString);

                boolean nodeExists;
                try(AutoCloseableLock readLock = readLock()) {
                    nodeExists = state.get(EntityType.STORAGE_NODE).containsKey(storageNode.getId());
                }

                if (nodeExists) {
                    log.warn("%s failed. Node '%s' exists.".formatted(RequestType.STORAGE_NODE_JOIN, storageNode.getId()));
                    yield CompletableFuture.failedFuture(new MetadataServerException("Node exists."));
                }

                try (AutoCloseableLock writeLock = writeLock()) {
                    state.get(EntityType.STORAGE_NODE).put(storageNode.getId(), storageNode);
                    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                }

                log.warn("%s succeeded. Node = '%s'.".formatted(RequestType.STORAGE_NODE_JOIN, storageNode.getId()));
                yield CompletableFuture.completedFuture(Message.EMPTY);
            }
            default -> CompletableFuture.failedFuture(new MetadataServerException("Unknown request type."));
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
        try(AutoCloseableLock writeLock = writeLock();
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

        try(AutoCloseableLock readLock = readLock()) {
            copy = new HashMap<>(state);
            last = getLastAppliedTermIndex();
        }

        final File snapshotFile =  storage.getSnapshotFile(last.getTerm(), last.getIndex());
        log.info("Taking a snapshot to file {}", snapshotFile);

        try(ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(FileUtils.newOutputStream(snapshotFile)))) {
            out.writeObject(copy);
        } catch(IOException ioe) {
            log.warn("Failed to write snapshot file \"" + snapshotFile + "\", last applied index=" + last);
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
        final RequestType command = RequestType.valueOf(messageParts[0]);

        return switch (command) {
            case TABLE_GET -> {
                String tableName = messageParts[1].split("=")[1];

                final Object result;
                try(AutoCloseableLock readLock = readLock()) {
                    result = state.get(EntityType.TABLE).getOrDefault(tableName, null);
                }

                log.debug("TABLE_GET: {} = {}", tableName, result);

                if (result == null) {
                    NotFoundResponse response = new NotFoundResponse();
                    yield CompletableFuture.completedFuture(response);
                } else {
                    Table table = (Table)result;
                    TableGetResponse response = new TableGetResponse(tableName, table.getNumPartitions());
                    yield CompletableFuture.completedFuture(response);
                }
            }
            default -> {
                yield CompletableFuture.failedFuture(new MetadataServerException("Unknown request type."));
            }
        };
    }
}

